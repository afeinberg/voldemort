package voldemort.utils;

import com.google.common.collect.Lists;
import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.versioning.Versioned;

import java.util.List;

public class ListStore<K, V> {
    private static final int DEFAULT_MAX_TRIES = 3;

    private final StoreClient<K, List<V>> store;
    private final int maxTries;

    public ListStore(StoreClient<K, List<V>> store) {
        this.store = store;
        this.maxTries = DEFAULT_MAX_TRIES;
    }

    public ListStore(StoreClient<K, List<V>> store, int maxTries) {
        this.store = store;
        this.maxTries = maxTries;
    }

    public Versioned<List<V>> get(K key) {
        return store.get(key);
    }

    public List<V> getValue(K key) {
        return store.getValue(key, Lists.<V>newArrayList());
    }

    public List<V> getValue(K key, List<V> defaultValue) {
        return store.getValue(key, defaultValue);
    }

    public boolean append(final K key, final V value) {
        UpdateAction<K, List<V>> update = new UpdateAction<K, List<V>>() {
            @Override
            public void update(StoreClient<K, List<V>> stc) {
                Versioned<List<V>> listVersioned = stc.get(key);
                if (listVersioned == null)
                    listVersioned = new Versioned<List<V>>(Lists.<V>newArrayList());

                listVersioned.getValue().add(value);
                stc.put(key, listVersioned);
            }
        };

        return store.applyUpdate(update, maxTries);
    }
}
