// SPDX-License-Identifier: GPL-2.0+
/*
 * ssr.c - RAID1 implementation for two physical devices
 *
 * Author: Robert Lică <37192540+Robert-ML@users.noreply.github.com>
 * Author: Andrei Preda <preda.andrei174@gmail.com>
 */
#include <linux/bio.h>
#include <linux/blk-mq.h>
#include <linux/blk_types.h>
#include <linux/blkdev.h>
#include <linux/buffer_head.h>
#include <linux/crc32.h>
#include <linux/cred.h>
#include <linux/fs.h>
#include <linux/genhd.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/pagemap.h>
#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/vmalloc.h>
#include <linux/wait.h>

#include "./ssr.h"

static struct block_device *pdsks[2];

static struct my_block_dev {
	struct blk_mq_tag_set tag_set;
	struct request_queue *queue;
	struct gendisk *gd;
	size_t size;
} g_dev;

struct work_bio_info {
	struct work_struct my_work;
	struct bio *original_bio;
};

struct workqueue_struct *queue;

static int my_block_open(struct block_device *bdev, fmode_t mode)
{
	return 0;
}

static void my_block_release(struct gendisk *gd, fmode_t mode)
{
}

/*
 * Read function to perform IO. It receives an unmapped page to write the disk
 * data data to.
 *
 * @page   : The page where to write the data.
 * @len    : The length of the data to read.
 * @offset : The offset in the page to write to.
 * @blk_dev: The block device to read from.
 * @sector : The sector of the block device to read from.
 */
static void read_page_from_disk(struct page *page, const size_t len,
					const size_t offset,
					struct block_device *blk_dev, sector_t sector)
{
	struct bio *read_bio;

	/* Set up a bio for reading from the disk. */
	read_bio = bio_alloc(GFP_NOIO, 1);
	read_bio->bi_disk = blk_dev->bd_disk;
	read_bio->bi_iter.bi_sector = sector;
	read_bio->bi_opf = REQ_OP_READ;

	bio_add_page(read_bio, page, len, offset);

	/* Do the reading. */
	submit_bio_wait(read_bio);

	bio_put(read_bio);
}

static void read_payload_from_disk(sector_t sector, unsigned long offset,
				   size_t len, struct block_device *blk_dev,
				   void *out_payload)
{
	struct page *page;
	u8 *buffer;

	page = alloc_page(GFP_NOIO);

	read_page_from_disk(page, len, offset, blk_dev, sector);

	/* Save the payload that was read in the page. */
	buffer = kmap_atomic(page);
	memcpy(out_payload + offset, buffer + offset, len);
	kunmap_atomic(buffer);

	__free_page(page);
}

/*
 * Write function to perform IO. It receives an unmapped page from which to
 * write the disk data.
 *
 * @page   : The page from where to take the data.
 * @len    : The length of the data to write.
 * @offset : The offset in the page to write from.
 * @blk_dev: The block device to write to.
 * @sector : The sector of the block device to write to.
 */
static void write_page_to_disk(struct page *page, const size_t len,
				const size_t offset,
				struct block_device *blk_dev, sector_t sector)
{
	struct bio *write_bio;

	/* Set up a bio for writing to the disk. */
	write_bio = bio_alloc(GFP_NOIO, 1);

	write_bio->bi_disk = blk_dev->bd_disk;
	write_bio->bi_iter.bi_sector = sector;
	write_bio->bi_opf = REQ_OP_WRITE;

	bio_add_page(write_bio, page, len, offset);

	submit_bio_wait(write_bio);

	bio_put(write_bio);
}

static void write_payload_to_disk(void *payload, size_t len, sector_t sector,
				  unsigned long offset,
				  struct block_device *blk_dev)
{
	struct page *page;
	u8 *buffer;

	/* Write the payload to the page of the bio. */
	page = alloc_page(GFP_NOIO);
	buffer = kmap_atomic(page);
	memcpy(buffer + offset, payload + offset, len);
	kunmap_atomic(buffer);

	/* Do the writing. */
	write_page_to_disk(page, len, offset, blk_dev, sector);

	__free_page(page);
}

static unsigned char payload[PAGE_SIZE];
static unsigned char payload0[PAGE_SIZE];
static unsigned char payload1[PAGE_SIZE];
static u32 crcs[CRC_PER_SECTOR];
static u32 crcs0[CRC_PER_SECTOR];
static u32 crcs1[CRC_PER_SECTOR];

static void my_read_handler(struct work_struct *work)
{
	struct work_bio_info *info;
	struct bio_vec bvec;
	struct bvec_iter i;

	bool both_disks_corrupted = false;

	info = container_of(work, struct work_bio_info, my_work);

	bio_for_each_segment (bvec, info->original_bio, i) {
		sector_t sector = i.bi_sector;
		unsigned long offset = bvec.bv_offset;
		size_t len = bvec.bv_len;

		sector_t crc_sector =
			LOGICAL_DISK_SECTORS + sector / CRC_PER_SECTOR;

		/* The position in the CRC sector where this page starts. */
		unsigned long crc_start_index = sector % CRC_PER_SECTOR;

		char *buffer;
		int i;

		/* Read the data and CRCs from both disks. */
		read_payload_from_disk(sector, offset, len, pdsks[0], payload0);
		read_payload_from_disk(sector, offset, len, pdsks[1], payload1);
		read_payload_from_disk(crc_sector, 0, KERNEL_SECTOR_SIZE,
				       pdsks[0], crcs0);
		read_payload_from_disk(crc_sector, 0, KERNEL_SECTOR_SIZE,
				       pdsks[1], crcs1);

		/*
		 * Each data sector needs to be checked against the CRC and
		 * updated individually. We do all these updates in memory first
		 * to avoid writing to the disk too many times.
		 */
		for (i = 0; i < len / KERNEL_SECTOR_SIZE; i += 1) {
			char *sector0 = payload0 + i * KERNEL_SECTOR_SIZE;
			char *sector1 = payload1 + i * KERNEL_SECTOR_SIZE;

			u32 stored0 = crcs0[crc_start_index + i];
			u32 real0 = crc32(0, sector0, KERNEL_SECTOR_SIZE);
			bool bad0 = stored0 != real0;

			u32 stored1 = crcs1[crc_start_index + i];
			u32 real1 = crc32(0, sector1, KERNEL_SECTOR_SIZE);
			bool bad1 = stored1 != real1;

			if (bad0 && bad1) {
				both_disks_corrupted = true;
				break;
			} else if (bad0 && !bad1) {
				memcpy(sector0, sector1, KERNEL_SECTOR_SIZE);
				crcs0[crc_start_index + i] = real1;
			} else if (bad1 && !bad0) {
				memcpy(sector1, sector0, KERNEL_SECTOR_SIZE);
				crcs1[crc_start_index + i] = real0;
			}
		}

		/* Write all data back to the disk, in case they got updated. */
		write_payload_to_disk(payload0, len, sector, offset, pdsks[0]);
		write_payload_to_disk(payload1, len, sector, offset, pdsks[1]);
		write_payload_to_disk(crcs0, KERNEL_SECTOR_SIZE, crc_sector, 0,
				      pdsks[0]);
		write_payload_to_disk(crcs1, KERNEL_SECTOR_SIZE, crc_sector, 0,
				      pdsks[1]);

		/* Send the requested data back. */
		buffer = kmap_atomic(bvec.bv_page);
		memcpy(buffer, payload0, len);
		kunmap_atomic(buffer);
	}

	if (unlikely(both_disks_corrupted)) {
		bio_io_error(info->original_bio);
	} else {
		bio_endio(info->original_bio);
	}

	kfree(info);
}

static void my_write_handler(struct work_struct *work)
{
	struct work_bio_info *info;
	struct bio_vec bvec;
	struct bvec_iter i;

	info = container_of(work, struct work_bio_info, my_work);

	bio_for_each_segment (bvec, info->original_bio, i) {
		sector_t sector = i.bi_sector;
		size_t data_len = bvec.bv_len;

		sector_t crc_sector =
			LOGICAL_DISK_SECTORS + sector / CRC_PER_SECTOR;

		/* The position in the CRC sector where this page starts. */
		unsigned long crc_start_index = sector % CRC_PER_SECTOR;

		unsigned char *data;
		int i;

		/* Write the data to both disks. */
		write_page_to_disk(bvec.bv_page, data_len, bvec.bv_offset, pdsks[0], sector);
		write_page_to_disk(bvec.bv_page, data_len, bvec.bv_offset, pdsks[1], sector);

		/* Recalculate and write the CRC for each sector of the page. */
		read_payload_from_disk(crc_sector, 0, KERNEL_SECTOR_SIZE,
				       pdsks[0], crcs);

		/* Map the data to make its CRC. */
		data = kmap_atomic(bvec.bv_page);

		for (i = 0; i < data_len / KERNEL_SECTOR_SIZE; i += 1) {
			crcs[crc_start_index + i] =
				crc32(0, data + i * KERNEL_SECTOR_SIZE,
				      KERNEL_SECTOR_SIZE);
		}
		/* Unmap the data */
		kunmap_atomic(data);

		/* Write the updated CRCs back to both disks */
		write_payload_to_disk(crcs, KERNEL_SECTOR_SIZE, crc_sector, 0,
					pdsks[0]);
		write_payload_to_disk(crcs, KERNEL_SECTOR_SIZE, crc_sector, 0,
					pdsks[1]);
	}

	bio_endio(info->original_bio);
	kfree(info);
}

static blk_qc_t my_submit_bio(struct bio *bio)
{
	int should_write = bio_data_dir(bio) == REQ_OP_WRITE;
	struct work_bio_info *info;

	info = kmalloc(sizeof(*info), GFP_ATOMIC);
	if (!info) {
		pr_alert("[SSR-E] Failed to allocate memory for work_bio_info");
		goto error_exit;
	}

	info->original_bio = bio;
	if (should_write) {
		INIT_WORK(&info->my_work, my_write_handler);
	} else {
		INIT_WORK(&info->my_work, my_read_handler);
	}
	queue_work(queue, &info->my_work);

	return BLK_QC_T_NONE;

error_exit:
	bio_endio(bio);
	return BLK_QC_T_NONE;
}

static const struct block_device_operations my_block_ops = {
	.owner = THIS_MODULE,
	.open = my_block_open,
	.release = my_block_release,
	.submit_bio = my_submit_bio,
};

static int create_block_device(struct my_block_dev *dev)
{
	int err;

	dev->size = LOGICAL_DISK_SIZE;

	/* Allocate queue. */
	dev->queue = blk_alloc_queue(NUMA_NO_NODE);
	if (IS_ERR(dev->queue)) {
		printk(KERN_ERR "blk_mq_init_queue: out of memory\n");
		err = -ENOMEM;
		goto out_blk_init;
	}
	blk_queue_logical_block_size(dev->queue, KERNEL_SECTOR_SIZE);
	dev->queue->queuedata = dev;

	/* initialize the gendisk structure */
	dev->gd = alloc_disk(SSR_NUM_MINORS);
	if (!dev->gd) {
		printk(KERN_ERR "alloc_disk: failure\n");
		err = -ENOMEM;
		goto out_alloc_disk;
	}

	dev->gd->major = SSR_MAJOR;
	dev->gd->first_minor = SSR_FIRST_MINOR;
	dev->gd->fops = &my_block_ops;
	dev->gd->queue = dev->queue;
	dev->gd->private_data = dev;
	snprintf(dev->gd->disk_name, DISK_NAME_LEN, LOGICAL_DISK_NAME);
	set_capacity(dev->gd, LOGICAL_DISK_SECTORS);

	add_disk(dev->gd);

	return 0;

out_alloc_disk:
	blk_cleanup_queue(dev->queue);
out_blk_init:
	blk_mq_free_tag_set(&dev->tag_set);
	return err;
}

static void delete_block_device(struct my_block_dev *dev)
{
	if (dev->gd) {
		del_gendisk(dev->gd);
		put_disk(dev->gd);
	}

	if (dev->queue)
		blk_cleanup_queue(dev->queue);
	if (dev->tag_set.tags)
		blk_mq_free_tag_set(&dev->tag_set);
}

static struct block_device *open_disk(char *name)
{
	struct block_device *bdev;

	bdev = blkdev_get_by_path(name, FMODE_READ | FMODE_WRITE | FMODE_EXCL,
				  THIS_MODULE);
	if (IS_ERR(bdev)) {
		printk(KERN_ERR "blkdev_get_by_path\n");
		return NULL;
	}

	return bdev;
}

static inline void close_disk(struct block_device *bdev)
{
	blkdev_put(bdev, FMODE_READ | FMODE_WRITE | FMODE_EXCL);
}

static int __init ssr_init(void)
{
	int err = 0;

	err = register_blkdev(SSR_MAJOR, LOGICAL_DISK_NAME);
	if (err < 0)
		return err;

	create_block_device(&g_dev);

	/* open physical disks */
	pdsks[0] = open_disk(PHYSICAL_DISK1_NAME);
	if (pdsks[0] == NULL) {
		goto remove_block_device;
	}
	pdsks[1] = open_disk(PHYSICAL_DISK2_NAME);
	if (pdsks[1] == NULL) {
		goto remove_block_device;
	}

	queue = create_singlethread_workqueue("myworkqueue");
	if (queue == NULL) {
		goto remove_disks;
	}

	return 0;

remove_disks:
	close_disk(pdsks[0]);
	close_disk(pdsks[1]);

remove_block_device:
	delete_block_device(&g_dev);

	unregister_blkdev(SSR_MAJOR, LOGICAL_DISK_NAME);

	return -ENXIO;
}

static void __exit ssr_exit(void)
{
	close_disk(pdsks[0]);
	close_disk(pdsks[1]);

	delete_block_device(&g_dev);

	unregister_blkdev(SSR_MAJOR, LOGICAL_DISK_NAME);
}

module_init(ssr_init);
module_exit(ssr_exit);

MODULE_DESCRIPTION("RAID1 implementation for two physical devices");
MODULE_AUTHOR("Robert Lică <37192540+Robert-ML@users.noreply.github.com>");
MODULE_AUTHOR("Andrei Preda <preda.andrei174@gmail.com>");
MODULE_LICENSE("GPL v2");
