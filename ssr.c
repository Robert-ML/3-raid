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

static int ssr_init(void)
{
	pr_info("Loading the module.\n");
	return 0;
}

static void ssr_exit(void)
{
	pr_info("Unloading the module.");
}

module_init(ssr_init);
module_exit(ssr_exit);

MODULE_DESCRIPTION("RAID1 implementation for two physical devices");
MODULE_AUTHOR("Robert Lică <37192540+Robert-ML@users.noreply.github.com>");
MODULE_AUTHOR("Andrei Preda <preda.andrei174@gmail.com>");
MODULE_LICENSE("GPL v2");
