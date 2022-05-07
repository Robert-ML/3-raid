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

static struct my_block_dev {
	struct blk_mq_tag_set tag_set;
	struct request_queue *queue;
	struct gendisk *gd;
	size_t size;
} g_dev;

static int my_block_open(struct block_device *bdev, fmode_t mode)
{
	return 0;
}

static void my_block_release(struct gendisk *gd, fmode_t mode)
{
}

static blk_qc_t my_submit_bio(struct bio *bio)
{
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

static int __init ssr_init(void)
{
	int err = 0;

	err = register_blkdev(SSR_MAJOR, LOGICAL_DISK_NAME);
	if (err < 0)
		return err;

	create_block_device(&g_dev);

	return 0;
}

static void __exit ssr_exit(void)
{
	delete_block_device(&g_dev);

	unregister_blkdev(SSR_MAJOR, LOGICAL_DISK_NAME);
}

module_init(ssr_init);
module_exit(ssr_exit);

MODULE_DESCRIPTION("RAID1 implementation for two physical devices");
MODULE_AUTHOR("Robert Lică <37192540+Robert-ML@users.noreply.github.com>");
MODULE_AUTHOR("Andrei Preda <preda.andrei174@gmail.com>");
MODULE_LICENSE("GPL v2");
