/*
 * Simple Software Raid - Linux header file
 */

#ifndef SSR_H_
#define SSR_H_ 1

#define SSR_MAJOR 240
#define SSR_FIRST_MINOR 0
#define SSR_NUM_MINORS 1

#define PHYSICAL_DISK1_NAME "/dev/vdb"
#define PHYSICAL_DISK2_NAME "/dev/vdc"

/* sector size */
#define KERNEL_SECTOR_SIZE 512

/* physical partition size - 95 MB (more than this results in error) */
#define LOGICAL_DISK_PATH "/dev/ssr"
#define LOGICAL_DISK_NAME "ssr"
#define LOGICAL_DISK_SIZE (95 * 1024 * 1024)
#define LOGICAL_DISK_SECTORS ((LOGICAL_DISK_SIZE) / (KERNEL_SECTOR_SIZE))

#define LOGICAL_DISK_CRC_SIZE (LOGICAL_DISK_SECTORS * sizeof(uint32_t))
#define LOGICAL_DISK_CRC_SECTORS (LOGICAL_DISK_CRC_SIZE / (KERNEL_SECTOR_SIZE))
#define CRC_SEED 0
#define CRC_PER_SECTOR (KERNEL_SECTOR_SIZE / sizeof(uint32_t))
#define get_crc_sector(ith_sect) (LOGICAL_DISK_SECTORS + ((ith_sect) / CRC_PER_SECTOR))
#define get_crc_index(ith_sect) ((ith_sect) % CRC_PER_SECTOR)

/* sync data */
#define SSR_IOCTL_SYNC 1

#endif
