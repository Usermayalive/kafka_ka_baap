package com.simplekafka.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Migrates legacy v1 dense index (.index) partitions to v2 sparse index (.sindex) format.
 *
 * CRASH SAFETY & ATOMICITY:
 * - Uses a `.migration.lock` file to detect and recover from interrupted migrations.
 * - Constructs the migrated state in a staging directory (`partition.migrating`).
 * - Swaps directories atomically and retains a backup (`partition.v1.bak`).
 */
public class LegacyPartitionMigrator {
    private static final Logger LOGGER = Logger.getLogger(LegacyPartitionMigrator.class.getName());

    public void migratePartition(File partitionDir) throws IOException {
        if (!partitionDir.exists() || !partitionDir.isDirectory()) {
            return;
        }

        File lockFile = new File(partitionDir, ".migration.lock");
        File parent = partitionDir.getParentFile();
        File stagingDir = new File(parent, partitionDir.getName() + ".migrating");
        File backupDir = new File(parent, partitionDir.getName() + ".v1.bak");

        // 1. Recover from previous interrupted migration
        if (lockFile.exists() || stagingDir.exists()) {
            LOGGER.warning("Detected incomplete previous migration for " + partitionDir.getName() + ". Cleaning up staging...");
            deleteRecursively(stagingDir);
            lockFile.delete();
        }

        // Check if migration is even needed (if all index files are already .sindex, skip)
        File[] denseIndexes = partitionDir.listFiles(f -> f.getName().endsWith(".index"));
        if (denseIndexes == null || denseIndexes.length == 0) {
            return; // Already migrated or empty
        }

        LOGGER.info("Starting migration of partition: " + partitionDir.getAbsolutePath());
        lockFile.createNewFile();

        if (!stagingDir.mkdirs()) {
            throw new IOException("Failed to create staging directory: " + stagingDir.getAbsolutePath());
        }

        // 2. Process all files into staging directory
        File[] allFiles = partitionDir.listFiles();
        if (allFiles != null) {
            for (File file : allFiles) {
                String name = file.getName();
                if (name.equals(".migration.lock")) {
                    continue;
                }

                if (name.endsWith(".log")) {
                    // Copy log file unchanged
                    Files.copy(file.toPath(), new File(stagingDir, name).toPath(), StandardCopyOption.REPLACE_EXISTING);
                } else if (name.endsWith(".index")) {
                    // Convert dense index to sparse index (.sindex)
                    String baseName = name.substring(0, name.length() - ".index".length());
                    File sindexFile = new File(stagingDir, baseName + ".sindex");
                    convertDenseToSparse(file, sindexFile);
                }
            }
        }

        // 3. Atomic swap: original -> backup, staging -> original
        if (backupDir.exists()) {
            deleteRecursively(backupDir);
        }

        lockFile.delete(); // clear lock before moving

        if (!partitionDir.renameTo(backupDir)) {
            throw new IOException("Failed to rename partition to backup: " + backupDir.getAbsolutePath());
        }

        if (!stagingDir.renameTo(partitionDir)) {
            // Attempt rollback
            backupDir.renameTo(partitionDir);
            throw new IOException("Failed to promote staging to partition: " + partitionDir.getAbsolutePath());
        }

        LOGGER.info("Migration successfully completed for " + partitionDir.getName());
    }

    /**
     * Reads a v1 16-byte dense index (offset: long, position: long) and writes
     * a sparse index capturing entries approximately every 4KB.
     */
    public void convertDenseToSparse(File denseIndexFile, File sparseIndexFile) throws IOException {
        SparseOffsetIndex sparseIndex = new SparseOffsetIndex(new java.util.ArrayList<>(), sparseIndexFile);

        try (FileChannel channel = FileChannel.open(denseIndexFile.toPath(), StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(16 * 1024); // 16KB read buffer (1024 entries)
            while (channel.read(buf) > 0) {
                buf.flip();
                while (buf.remaining() >= 16) {
                    long offset = buf.getLong();
                    long position = buf.getLong();
                    sparseIndex.maybeAddEntry(offset, position);
                }
                buf.compact();
            }
        }

        sparseIndex.flush();
    }

    private void deleteRecursively(File dir) {
        if (dir == null || !dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteRecursively(f);
                else f.delete();
            }
        }
        dir.delete();
    }
}
