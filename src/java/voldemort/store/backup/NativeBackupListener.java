package voldemort.store.backup;

public interface NativeBackupListener {

    public void notify(String message);

    public void warn(String message);

    public void backupFiles(int numFiles, long size);

    public void copyFile(String name, long total, long size);

    public void finished();
}
