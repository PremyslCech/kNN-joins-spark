package cz.siret.knn.model;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Pair implements Serializable {

	public int key;
	public float value;
	
	public Pair() {
	}
	
	public Pair(int key, float value) {
		this.key = key;
		this.value = value;
	}
		
	@Override
	public String toString() {
		return String.format("%1$d:%2$f", key, value); 
	}

	public int getKey() {
		return key;
	}
	
	public float getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		return (int) (1000*1000*value + key);
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof Pair) {
			Pair p = (Pair) o;
			return key == p.key && value == p.value;
		} else {
			return false;
		}
	}
	
}
