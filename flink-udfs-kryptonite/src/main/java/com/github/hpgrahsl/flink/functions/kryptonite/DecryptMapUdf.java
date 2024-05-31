package com.github.hpgrahsl.flink.functions.kryptonite;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

public class DecryptMapUdf extends AbstractCipherFieldUdf {

    @SuppressWarnings("unchecked")
    public @Nullable <V> Map<?,V> eval(@Nullable final Object data, final V valueType) { 
        if(data == null || !(data instanceof Map)) {
            return null;
        }
        return ((Map<?,String>)data).entrySet().stream()
              .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(),(V)decryptData(e.getValue())))
              .collect(LinkedHashMap::new,(lhm, e) -> lhm.put(e.getKey(),e.getValue()), HashMap::putAll);
    }

    @Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		return TypeInference.newBuilder()
				.inputTypeStrategy(
                    InputTypeStrategies.sequence(
                        InputTypeStrategies.ANY, //SHOULD BE LIMITED to "any map" i.e. MAP<K,V>
                        InputTypeStrategies.ANY
                    )
                )
                .outputTypeStrategy(ctx -> {
                    var targetKeyType = ((KeyValueDataType)ctx.getArgumentDataTypes().get(0)).getKeyDataType();
                    var targetValueType = ctx.getArgumentDataTypes().get(1);
                    return Optional.of(DataTypes.MAP(targetKeyType,targetValueType));
                })
				.build();
	}

}
