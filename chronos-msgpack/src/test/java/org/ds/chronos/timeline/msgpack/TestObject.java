package org.ds.chronos.timeline.msgpack;

import org.msgpack.annotation.Message;

@Message
class TestObject implements Timestamped {

	private int id;
	private String name;
	private int age;
	private double weight;
	private long birthdate;

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id
	 *          the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *          the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the age
	 */
	public int getAge() {
		return age;
	}

	/**
	 * @param age
	 *          the age to set
	 */
	public void setAge(int age) {
		this.age = age;
	}

	/**
	 * @return the weight
	 */
	public double getWeight() {
		return weight;
	}

	/**
	 * @param weight
	 *          the weight to set
	 */
	public void setWeight(double weight) {
		this.weight = weight;
	}

	@Override
	public long getTimestamp() {
		return birthdate;
	}

	@Override
	public void setTimestamp(long timestamp) {
		birthdate = timestamp;
	}
}
