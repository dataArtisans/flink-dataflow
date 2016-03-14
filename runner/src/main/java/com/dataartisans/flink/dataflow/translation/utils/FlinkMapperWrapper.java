package com.dataartisans.flink.dataflow.translation.utils;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class FlinkMapperWrapper<KI, VI, KO, VO> extends RichMapPartitionFunction<Tuple2<KI, VI>, Tuple2<KO, VO>> implements ResultTypeQueryable<Tuple2<KO, VO>> {

	
	private final DoFn<KV<KI, VI>, KV<KO,VO>> fn;
	private transient TypeInformation<Tuple2<KO, VO>> typeInformation;

	public FlinkMapperWrapper(DoFn<KV<KI, VI>, KV<KO, VO>> fn) {
		this.fn = fn;
		
		Type out = ((ParameterizedType) fn.getClass().getGenericSuperclass()).getActualTypeArguments()[1];
		Type kot = ((ParameterizedType) out).getActualTypeArguments()[0];
		Type vot = ((ParameterizedType) out).getActualTypeArguments()[1];
		
		
		TypeInformation<KO> koi = (TypeInformation<KO>) TypeExtractor.createTypeInfo(kot);
		TypeInformation<VO> voi = (TypeInformation<VO>) TypeExtractor.createTypeInfo(vot);
		
		typeInformation = new TupleTypeInfo<>(koi, voi);
	}

	@Override
	public TypeInformation<Tuple2<KO, VO>> getProducedType() {
		return typeInformation;
	}

	@Override
	public void mapPartition(Iterable<Tuple2<KI, VI>> values, Collector<Tuple2<KO, VO>> out) throws Exception {

	}
}
