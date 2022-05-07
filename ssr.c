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
	struct bio *sect;
	struct bio *crc;
};

struct workqueue_struct *queue;

static int my_block_open(struct block_device *bdev, fmode_t mode)
{
	return 0;
}

static void my_block_release(struct gendisk *gd, fmode_t mode)
{
}

static void my_bio_handler(struct work_struct *work)
{
	struct work_bio_info *info;
	struct page *page;
	char *buffer;

	info = container_of(work, struct work_bio_info, my_work);

	page = alloc_page(GFP_NOIO);
	bio_add_page(&info->sect[0], page, KERNEL_SECTOR_SIZE, 0);

	if (info->sect[0].bi_opf == REQ_OP_WRITE) {
		void *data = bio_data(&info->sect[0]);
		buffer = kmap_atomic(page);
		strcpy(buffer, data);
		kunmap_atomic(buffer);
	}

	submit_bio_wait(&info->sect[0]);

	if (info->sect[0].bi_opf == REQ_OP_READ) {
		buffer = kmap_atomic(page);
		kunmap_atomic(buffer);
	}

	bio_endio(info->original_bio);

	bio_put(&info->sect[0]);
	__free_page(page);

	kfree(info);
}

static blk_qc_t my_submit_bio(struct bio *bio)
{
	/* 0 - read | 1 - write */
	size_t i = 0;
	int dir = bio_data_dir(bio);
	struct bio *sect;
	struct bio *crc;
	struct work_bio_info *info;

	/* alloc bios */
	sect = bio_alloc(GFP_NOIO, ARRAY_SIZE(pdsks) + 2);
	crc = bio_alloc(GFP_NOIO, 2);
	if (sect == NULL) {
		pr_alert("[ERROR]: Bio allocation failed!");
		return -ENOMEM;
	}

	/* init bios */
	for (i = 0; i < ARRAY_SIZE(pdsks); i++) {
		sect[i].bi_disk = pdsks[i]->bd_disk;
		sect[i].bi_iter.bi_sector = bio->bi_iter.bi_sector;
		sect[i].bi_opf = dir;

		crc[i].bi_disk = pdsks[i]->bd_disk;
		crc[i].bi_iter.bi_sector =
			get_crc_sector(bio->bi_iter.bi_sector);
		crc[i].bi_opf = dir;
	}

	/* submit sect and crc to workqueue */
	info = kmalloc(sizeof(*info), GFP_ATOMIC);
	info->original_bio = bio;
	info->sect = sect;
	info->crc = crc;
	INIT_WORK(&info->my_work, my_bio_handler);
	queue_work(queue, &info->my_work);

	return 0;
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

static int __init ssr_init(void)
{
	int err = 0;

	err = register_blkdev(SSR_MAJOR, LOGICAL_DISK_NAME);
	if (err < 0)
		return err;

	create_block_device(&g_dev);

	// open physical disks
	pdsks[0] = open_disk(PHYSICAL_DISK1_NAME);
	if (pdsks[0] == NULL) {
		goto remove_block_device;
	}
	pdsks[1] = open_disk(PHYSICAL_DISK2_NAME);
	if (pdsks[1] == NULL) {
		goto remove_block_device;
	}

	queue = create_singlethread_workqueue("myworkqueue");

	return 0;

remove_block_device:
	delete_block_device(&g_dev);

	unregister_blkdev(SSR_MAJOR, LOGICAL_DISK_NAME);

	return -ENXIO;
}

static void close_disk(struct block_device *bdev)
{
	/* TODO 4/1: put block device */
	blkdev_put(bdev, FMODE_READ | FMODE_WRITE | FMODE_EXCL);
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
