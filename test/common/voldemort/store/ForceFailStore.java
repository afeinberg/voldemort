package voldemort.store;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Map;

/**
 * A store which can specify whether some or all operations can be failed.
 */
public class ForceFailStore<K, V> extends DelegatingStore<K, V> {

    private final VoldemortException exception;

    private volatile boolean failDelete = false;
    private volatile boolean failGetAll = false;
    private volatile boolean failGet = false;
    private volatile boolean failPut = false;
    private volatile boolean failGetVersions = false;
    private volatile boolean failAll = false;

    public ForceFailStore(Store<K, V> innerStore) {
        this(innerStore, new VoldemortException("Operation failed!"));
    }

    public ForceFailStore(Store<K, V> innerStore, VoldemortException e) {
        super(innerStore);
        this.exception = e;
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        if (failDelete || failAll)
            throw exception;

        return super.delete(key, version);
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        if (failGetAll || failAll)
            throw exception;

        return super.getAll(keys);
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        if (failGet || failAll)
            throw exception;

        return super.get(key);
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        if (failPut || failAll)
            throw exception;

        super.put(key, value);
    }

    @Override
    public List<Version> getVersions(K key) {
        if (failGetVersions || failAll)
            throw exception;

        return super.getVersions(key);
    }

    public void setFailDelete(boolean failDelete) {
        this.failDelete = failDelete;
    }

    public void setFailGetAll(boolean failGetAll) {
        this.failGetAll = failGetAll;
    }

    public void setFailGet(boolean failGet) {
        this.failGet = failGet;
    }

    public void setFailPut(boolean failPut) {
        this.failPut = failPut;
    }

    public void setFailGetVersions(boolean failGetVersions) {
        this.failGetVersions = failGetVersions;
    }

    public void setFailAll(boolean failAll) {
        this.failAll = failAll;
    }
}
