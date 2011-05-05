package voldemort.store.quota;

import java.io.File;

public class QuotaUtils {

    /**
     * Compute disk space occupied by all files in a directory
     *
     * @param path Path to the directory
     * @return Total bytes occupied by files in a directory
     */
    public static long diskUtilization(String path) {
        File directory = new File(path);
        if(!directory.isDirectory())
            throw new IllegalStateException(path + " is not a directory!");

        long total = 0;
        for(File file: directory.listFiles()) {
            if(file.isFile())
                total += file.length();
        }

        return total;
    }

    /**
     * Compute disk space occupied by all files in a directory, when the
     * size of each file is known ahead of time
     *
     * @param path Path to the directory
     * @param bytesPerFile Size (in bytes) of each file
     * @return Total bytes occupied by files in a directory
     */
    public static long diskUtilization(String path, long bytesPerFile) {
        File directory = new File(path);
        if(!directory.isDirectory())
            throw new IllegalStateException(path + " is not a directory!");

        long total = 0;
        for(File file: directory.listFiles()) {
            if(file.isFile())
                total += bytesPerFile;
        }

        return total;
    }
}
