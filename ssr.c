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

#define _in
#define _out

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

static void read_payload_from_disk(sector_t sector, unsigned long offset,
				   size_t len, struct block_device *blk_dev,
				   char *out_payload)
{
	struct bio *read_bio;
	struct page *page;
	char *buffer;

	/* Set up a bio for reading from the disk. */
	read_bio = bio_alloc(GFP_NOIO, 1);
	read_bio->bi_disk = blk_dev->bd_disk;
	read_bio->bi_iter.bi_sector = sector;
	read_bio->bi_opf = REQ_OP_READ;
	page = alloc_page(GFP_NOIO);
	bio_add_page(read_bio, page, len, offset);

	/* Do the reading. */
	submit_bio_wait(read_bio);

	/* Save the payload that was read in the page. */
	buffer = kmap_atomic(page);
	memcpy(out_payload + offset, buffer + offset, len);
	kunmap_atomic(buffer);

	bio_put(read_bio);
	__free_page(page);
}

/*
 * compute_crcs_from_buffer
 *
 * Compute the CRCs for the passed "buffer" of length "len" and the size on
 * which a CRC is calculated is "sector_size"
 *
 * @param buffer - the buffer to compute the CRCs for
 * @param len - the length of the buffer
 * @param sector_size - the size of the sector on which the CRC is calculated
 * @param crcs - the array where the CRCs will be saved to
 *
 * Note: "crcs" Must be of length "len / sector_size"
 */
static void compute_crcs_from_buffer(_in char *buffer, _in const size_t len,
					_in const size_t sector_size, _out u32 *crcs)
{
	size_t i;

	for (i = 0; i < len / sector_size; i++) {
		crcs[i] = crc32(CRC_SEED, buffer + i * sector_size, sector_size);
	}
}

static char payload[PAGE_SIZE];
static char crcs[PAGE_SIZE];
static u32 crcs_comp[PAGE_SIZE / KERNEL_SECTOR_SIZE];
static u32 crcs_read[PAGE_SIZE / KERNEL_SECTOR_SIZE];
static void my_read_handler(struct work_struct *work)
{
	struct work_bio_info *info;
	struct bio_vec bvec;
	struct bvec_iter it;

	size_t i;

	info = container_of(work, struct work_bio_info, my_work);


	sector_t sector = 0;
	size_t len = PAGE_SIZE;
	unsigned long offset = 0;

	read_payload_from_disk(sector, offset, len, pdsks[0], payload);

	printk(KERN_INFO "Read sector %llu from disk:\n", sector);
	for (i = 0; i < KERNEL_SECTOR_SIZE; ++i) {
		printk(KERN_CONT "%02x", payload[i]);
		if (i % 64 == 63)
			printk(KERN_CONT "\n");
	}
	printk(KERN_CONT "\n");
	printk(KERN_INFO "Read sector %llu from disk:\n", sector + 1);
	for (i = KERNEL_SECTOR_SIZE; i < KERNEL_SECTOR_SIZE * 2; ++i) {
		printk(KERN_CONT "%02x", payload[i]);
		if (i % 64 == 63)
			printk(KERN_CONT "\n");
	}
	printk(KERN_CONT "\n");


	u32 crc1 = crc32(CRC_SEED, payload, KERNEL_SECTOR_SIZE);
	u32 crc2 = crc32(CRC_SEED, payload + KERNEL_SECTOR_SIZE, KERNEL_SECTOR_SIZE);
	u8 *p_crc1 = (u8 *)&crc1;
	u8 *p_crc2 = (u8 *)&crc2;
	printk(KERN_INFO "CRC1: ");
	for(i = 0; i < 4; ++i) {
		printk(KERN_CONT "%02x", p_crc1[i]);
	}
	printk(KERN_CONT "\n");

	printk(KERN_INFO "CRC2: ");
	for(i = 0; i < 4; ++i) {
		printk(KERN_CONT "%02x", p_crc2[i]);
	}
	printk(KERN_CONT "\n");

	sector = 194560;
	len = PAGE_SIZE;
	offset = 0;

	read_payload_from_disk(sector, offset, len, pdsks[0], crcs);

	pr_info("Read CRC sector %llu from disk:\n", sector);
	for (i = 0; i < KERNEL_SECTOR_SIZE; ++i) {
		pr_info("%02x", crcs[i]);
		// if (i % 32 == 31)
		// 	printk(KERN_CONT "\n");
	}
	pr_info("\n");

	bio_endio(info->original_bio);
	kfree(info);
	return;

	// bio_for_each_segment (bvec, info->original_bio, it) {
	// 	sector_t sector = it.bi_sector;
	// 	unsigned long offset = bvec.bv_offset;
	// 	size_t len = bvec.bv_len;
	// 	char *buffer;

	// 	size_t i;

	// 	pr_info("\tsector: %llu, offset: %lu, len: %u\n", sector, offset, len);

	// 	sector_t crc_sector;
	// 	unsigned long crc_index_in_sector;

	// 	bool first_read_faulty = false;

	// 	/* we check to not request a sector out of the disk size */
	// 	crc_sector = get_crc_sector(sector);
	// 	crc_sector = min(crc_sector, (u64)(LOGICAL_DISK_SECTORS + LOGICAL_DISK_CRC_SECTORS) - (PAGE_SIZE / SECTOR_SIZE));
	// 	/*
	// 	 * crc_index_in_sector can be used to also extract the needed CRCs from
	// 	 * the loaded page of CRCs
	// 	 */
	// 	crc_index_in_sector = get_crc_offset(sector);

	// 	/* Assume the first block device has the right data. */
	// 	read_payload_from_disk(sector, offset, len, pdsks[0], payload);
	// 	read_payload_from_disk(crc_sector, 0, PAGE_SIZE, pdsks[0], crcs);


	// 	compute_crcs_from_buffer(payload, ARRAY_SIZE(payload), KERNEL_SECTOR_SIZE, crcs_comp);
	// 	// pr_info("crc_index_in_sector: %lu\n", crc_index_in_sector);
	// 	memcpy(crcs_read, &(((u32 *)crcs)[crc_index_in_sector]), ARRAY_SIZE(crcs_read));

	// 	pr_info("Computed crcs (base + offset: crc_computed) :\n");
	// 	for (i = 0; i < ARRAY_SIZE(crcs_comp); ++i) {
	// 		printk(KERN_INFO "%llu + %lu: %u, ", crc_sector, (crc_index_in_sector + i), crcs_comp[i]);
	// 	}
	// 	pr_info("\n\n");

	// 	pr_info("crcs read from disk (base + offset: crc_read):\n");
	// 	for (i = 0; i < ARRAY_SIZE(crcs) / sizeof(u32); ++i) {
	// 		printk(KERN_INFO "%llu + %u: %u, ", crc_sector, i, crcs[i]);
	// 	}
	// 	pr_info("\n\n");

	// 	// for (i = 0; i < PAGE_SIZE / KERNEL_SECTOR_SIZE; ++i) {
	// 	// 	pr_info("\tcrcs_comp[%u]: %u | crcs_read[%u]: %u\n", i, crcs_comp[i], i, crcs_read[i]);
	// 	// 	if (crcs_comp[i] != crcs_read[i]) {
	// 	// 		first_read_faulty = true;
	// 	// 		pr_info("\t\t[NOTE]: CRC mismatch at sector %llu\n", sector + i);
	// 	// 		// break;
	// 	// 	}
	// 	// }

	// 	/* Send the requested data back. */
	// 	buffer = kmap_atomic(bvec.bv_page);
	// 	memcpy(buffer, payload, len);
	// 	kunmap_atomic(buffer);

	// 	break;
	// }

	// bio_endio(info->original_bio);
	// kfree(info);
}

static void write_payload_to_disk(char *payload, size_t len, sector_t sector,
				  unsigned long offset,
				  struct block_device *blk_dev)
{
	struct bio *write_bio;
	struct page *page;
	char *buffer;

	/* Set up a bio for writing to the disk. */
	write_bio = bio_alloc(GFP_NOIO, 1);
	write_bio->bi_disk = blk_dev->bd_disk;
	write_bio->bi_iter.bi_sector = sector;
	write_bio->bi_opf = REQ_OP_WRITE;

	/* Write the payload to the page of the bio. */
	page = alloc_page(GFP_NOIO);
	bio_add_page(write_bio, page, len, offset);
	buffer = kmap_atomic(page);
	memcpy(buffer + offset, payload + offset, len);
	kunmap_atomic(buffer);

	/* Do the writing. */
	submit_bio_wait(write_bio);

	bio_put(write_bio);
	__free_page(page);
}

static void my_write_handler(struct work_struct *work)
{
	struct work_bio_info *info;
	struct bio_vec bvec;
	struct bvec_iter i;

	pr_info("my_write_handler\n");

	info = container_of(work, struct work_bio_info, my_work);

	bio_for_each_segment (bvec, info->original_bio, i) {
		sector_t sector = i.bi_sector;
		unsigned long offset = bvec.bv_offset;
		size_t len = bvec.bv_len;

		u32 crc;
		sector_t crc_sector;
		unsigned long crc_offset;
		int i;

		/* Copy the data from the user(?). */
		unsigned char payload[4096];
		char *buffer = kmap_atomic(bvec.bv_page);
		memcpy(payload, buffer, len);
		kunmap_atomic(buffer);

		/* Write the data to both disks. */
		write_payload_to_disk(payload, len, sector, offset, pdsks[0]);
		write_payload_to_disk(payload, len, sector, offset, pdsks[1]);

		/* Update the CRCs. */
		crc = crc32(0, payload, KERNEL_SECTOR_SIZE);
		crc_sector = LOGICAL_DISK_SECTORS + sector / CRC_PER_SECTOR;
		crc_offset = sector % CRC_PER_SECTOR;
		read_payload_from_disk(crc_sector, 0, len, pdsks[0], payload);
		for (i = 0; i < CRC_PER_SECTOR; i += 1) {
			((u32 *)payload)[i] = crc;
		}
		write_payload_to_disk(payload, len, crc_sector, 0, pdsks[0]);
		write_payload_to_disk(payload, len, crc_sector, 0, pdsks[1]);
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

	pr_info("RUN 1\n");
	pr_info("[INFO]: KERNEL_SECTOR_SIZE: %d | PAGE_SIZE: %ld\n", KERNEL_SECTOR_SIZE, PAGE_SIZE);
	pr_info("[INFO]: LOGICAL_DISK_SIZE: %d | LOGICAL_DISK_SECTORS: %d\n", LOGICAL_DISK_SIZE, LOGICAL_DISK_SECTORS);
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
