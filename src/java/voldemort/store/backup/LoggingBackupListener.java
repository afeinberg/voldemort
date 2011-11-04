package voldemort.store.backup;

import org.apache.log4j.Logger;

public class LoggingBackupListener implements NativeBackupListener {

    private final Logger logger;

    public LoggingBackupListener(Class<?> parentCls) {
        this.logger = Logger.getLogger(parentCls);
    }

    public void notify(String message) {
        logger.info(message);
    }

    public void warn(String message) {
        logger.warn(message);
    }

    public void backupFiles(int numFiles, long size) {

    }

    public void copyFile(String name, long total, long size) {

    }

    public void finished() {

    }
}
