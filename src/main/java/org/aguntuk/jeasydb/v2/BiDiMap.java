package org.aguntuk.jeasydb.v2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class BiDiMap<K, V> implements Map<K, V> {

	private Map<K, V> innerMap;
	private Map<V, K> reverseMap;

	public BiDiMap() {
		innerMap = new HashMap<K, V>();
		reverseMap = new HashMap<V, K>();
	}

	public BiDiMap(Map<K, V> source) {
		this();
		initialize(source);
		this.innerMap = source;
	}

	private void initialize(Map<K, V> data) {
		for (Iterator<Map.Entry<K, V>> iter = data.entrySet().iterator(); iter.hasNext();) {
			Entry<K, V> e = iter.next();
			reverseMap.put(e.getValue(), e.getKey());
		}
	}

	@Override
	public int size() {
		return innerMap.size();
	}

	@Override
	public boolean isEmpty() {
		return innerMap.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return innerMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return innerMap.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return innerMap.get(key);
	}

	@Override
	public V put(K key, V value) {
		reverseMap.put(value, key);
		return innerMap.put(key, value);
	}

	@Override
	public V remove(Object key) {
		V value = innerMap.get(key);
		reverseMap.remove(value);
		return innerMap.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		m.entrySet().stream().map(v -> {
			reverseMap.put(v.getValue(), v.getKey());
			return 0;
		});
		innerMap.putAll(m);
	}

	@Override
	public void clear() {
		innerMap.clear();
	}

	@Override
	public Set<K> keySet() {
		return innerMap.keySet();
	}

	@Override
	public Collection<V> values() {
		return innerMap.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return innerMap.entrySet();
	}

	public K getKey(V value) {
		return reverseMap.get(value);
	}

}
