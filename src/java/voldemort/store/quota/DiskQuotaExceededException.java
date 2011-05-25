package voldemort.store.quota;

public class DiskQuotaExceededException extends QuotaExceededException {

    private static final long serialVersionUID = 1L;

    public DiskQuotaExceededException(String message) {
        super(message);
    }

    public DiskQuotaExceededException(String message, Exception cause) {
        super(message, cause);
    }

    /**
     * Override to avoid the overhead of stack trace. For a given store there is
     * really only one method (put) that can throw this so retaining the stack
     * trace is not useful
     */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}