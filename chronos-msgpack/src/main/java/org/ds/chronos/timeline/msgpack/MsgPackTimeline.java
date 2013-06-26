package org.ds.chronos.timeline.msgpack;

import org.ds.chronos.api.Chronicle;
import org.ds.chronos.timeline.SimpleTimeline;
import org.msgpack.MessagePack;

/**
 * JSON Timeline
 * 
 * @author Dan Simpson
 * 
 * @param <T>
 *          type which can be serialized to and from json
 */
public class MsgPackTimeline<T extends Timestamped> extends SimpleTimeline<T> {

	public static final MessagePack msgpack = new MessagePack();

	/**
	 * Create a new JSON Timeline
	 * 
	 * @param chronicle
	 *          the underlying storage
	 * @param ref
	 *          the typeref for decoding
	 */
	public MsgPackTimeline(Chronicle chronicle, Class<T> klass) {
		super(chronicle, new MsgPackTimelineDecoder<T>(klass), new MsgPackTimelineEncoder<T>());
	}

}
