package voldemort.store.quota;


import voldemort.VoldemortApplicationException;

public class QuotaExceededException extends VoldemortApplicationException {

    private static final long serialVersionUID = 1L;

    public QuotaExceededException(String s, Throwable t) {
        super(s, t);
    }

    public QuotaExceededException(String s) {
        super(s);
    }

    public QuotaExceededException(Throwable t) {
        super(t);
    }
}
