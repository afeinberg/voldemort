package voldemort.store.bdb;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.util.DbBackup;
import voldemort.VoldemortException;
import voldemort.store.backup.NativeBackupListener;
import voldemort.utils.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;

public class BdbNativeBackup {

    private static final String EXTENSION = ".jdb";

    private final Environment environment;
    private final File environmentDir;
    private final File backupDir;
    private final NativeBackupListener listener;

    public BdbNativeBackup(Environment environment,
                           File backupDir,
                           NativeBackupListener listener) {
        Utils.notNull(environment);

        this.environment = environment;
        this.environmentDir = environment.getHome();
        this.backupDir = backupDir;
        this.listener = listener;
    }

    /**
     * Perform backup
     */
    public void performBackup() {
        Long lastBackup = determineLastFile();
        DbBackup backup;
        try {
            backup = lastBackup == null ? new DbBackup(environment)
                                        : new DbBackup(environment, lastBackup);
            backup.startBackup();
            try {
                String[] files = backup.getLogFilesInBackupSet();
                backupFiles(files);
            } finally {
                backup.endBackup();
            }
        } catch(DatabaseException e) {
            throw new VoldemortException("Error performing native backup", e);
        }
    }

    /**
     * Determine the last file that was backed up
     *
     * @return Log id of the last file or null if none
     */
    public Long determineLastFile() {
        listener.notify("Determining the last file backed up...");
        File[] files = backupDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if(!name.endsWith(EXTENSION)) {
                    return false;
                }
                String part = getLogId(name);
                try {
                    Long.parseLong(part);
                } catch(NumberFormatException e) {
                    listener.warn("Warning: "
                                  + EXTENSION
                                  + " file whose name is not a number, ignoring: "
                                  + name);
                }
                return true;
            }
        });
        if(files.length == 0) {
            listener.notify("No backup files found, assuming full backup required.");
            return null;
        }
        long largest = Long.MIN_VALUE;
        for(File file: files) {
            long value = fileNameToLong(file.getName());
            if(value > largest) {
                largest = value;
            }
        }
        listener.notify("Last backed up file was " + largest);
        return largest;
    }

    /**
     * Backup individual BDB log files
     *
     * @param files Files to backup
     */
    public void backupFiles(String[] files) {
        long size = 0;
        for(String name: files) {
            size += new File(environmentDir, name).length();
        }
        listener.backupFiles(files.length, size);
        Arrays.sort(files, new Comparator<String>() {
            public int compare(String o1, String o2) {
                long result = fileNameToLong(o1) - fileNameToLong(o2);
                if(result < 0) {
                    return -1;
                } else if (result > 0) {
                    return 1;
                }
                return 0;
            }
        });
        long total = 0;
        for(String name: files) {
            File source = new File(environmentDir, name);
            File dest = new File(backupDir, name);
            listener.copyFile(name, total, size);
            try {
                copyFile(source, dest);
            } catch(IOException e) {
                if(dest.exists()) {
                    dest.delete();
                }
                throw new VoldemortException("Error while copying "
                                             + name
                                             + ". Deleting to ensure we don't have a corrupt backup",
                                             e);
            }
            total += source.length();
        }
    }

    /**
     * Efficient copy using {@link FileChannel#transferFrom}
     *
     * @param sourceFile
     * @param destFile
     * @throws IOException If unable to copy the files
     */
    private void copyFile(File sourceFile, File destFile) throws IOException {
        if(!destFile.exists()) {
            assert(destFile.createNewFile());
        }
        FileChannel source = null;
        FileChannel dest = null;
        try {
            source = new FileInputStream(sourceFile).getChannel();
            dest = new FileOutputStream(destFile).getChannel();
            dest.transferFrom(source, 0, source.size());
        } finally {
            if(source != null) {
                try {
                    source.close();
                } finally {
                    if(dest != null) {
                        dest.close();
                    }
                }
            }
        }
    }

    private static long fileNameToLong(String name) {
        String part = getLogId(name);
        return Long.parseLong(part);
    }

    private static String getLogId(String name) {
        return name.substring(0, name.length() - EXTENSION.length());
    }
}
