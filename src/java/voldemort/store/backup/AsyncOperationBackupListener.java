package voldemort.store.backup;

import voldemort.server.protocol.admin.AsyncOperationStatus;

public class AsyncOperationBackupListener implements NativeBackupListener {

    private final AsyncOperationStatus status;

    public AsyncOperationBackupListener(AsyncOperationStatus status) {
        this.status = status;
    }

    public void notify(String message) {
        status.setStatus(message);
    }

    public void warn(String message) {
        status.setStatus(message);
    }

    public void backupFiles(int numFiles, long size) {
        double sizeMb = toMb(size);
        status.setStatus(String.format("Backup up %d files with a total of %.1fmb",
                                       numFiles,
                                       sizeMb));
    }

    private static double toMb(double size) {
        return (double) size / 1048576;
    }

    public void copyFile(String name, long total, long size) {
        status.setStatus(String.format("%3d%% Copying %s", total * 100 / size, name));
    }

    public void finished() {

    }
}
