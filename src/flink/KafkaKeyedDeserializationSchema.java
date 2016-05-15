package flink;

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

public class KafkaKeyedDeserializationSchema implements
		KeyedDeserializationSchema<Tuple2<String, String>> {

	private static final long serialVersionUID = -5128388939592570755L;

	@Override
	public TypeInformation<Tuple2<String, String>> getProducedType() {
		// TODO Auto-generated method stub
		return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);
	}

	@Override
	public Tuple2<String, String> deserialize(byte[] messageKey,
			byte[] message, String topic, int partition, long offset)
			throws IOException {
		return new Tuple2<String, String>(new String(messageKey), new String(
				message));
	}

	@Override
	public boolean isEndOfStream(Tuple2<String, String> nextElement) {
		// TODO Auto-generated method stub
		return false;
	}
}
