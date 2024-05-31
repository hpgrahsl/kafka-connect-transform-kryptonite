package com.github.hpgrahsl.flink.functions.kryptonite;

import java.util.Optional;

import org.apache.flink.table.functions.FunctionContext;
import com.github.hpgrahsl.kryptonite.FieldMetaData;
import com.github.hpgrahsl.kryptonite.crypto.tink.TinkAesGcm;

public class EncryptArrayUdf extends AbstractCipherFieldUdf {

    private final static String CIPHER_ALGORITHM_DEFAULT = TinkAesGcm.CIPHER_ALGORITHM;

    private transient String defaultCipherDataKeyIdentifier;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        //TODO: load default key id via the function context
        defaultCipherDataKeyIdentifier = "keyA";
    }

    public String[] eval(final String[] data) {
        return process(data);
    }

    public String[] eval(final Boolean[] data) {
        return process(data);
    }

    public String[] eval(final Integer[] data) {
        return process(data);
    }

    public String[] eval(final Long[] data) {
        return process(data);
    }

    public String[] eval(final Float[] data) {
        return process(data);
    }

    public String[] eval(final Double[] data) {
        return process(data);
    }

    public String[] eval(final Byte[] data) {
        return process(data);
    }

    private <T> String[] process(T[] array) {
        if(array == null ) {
            return null;
        }
        var dataEnc = new String[array.length];
        for(int s = 0; s < array.length; s++) {
            dataEnc[s] = encryptData(array[s],new FieldMetaData(
                            CIPHER_ALGORITHM_DEFAULT,
                            Optional.ofNullable(array[s]).map(o -> o.getClass().getName()).orElse(""),
                            defaultCipherDataKeyIdentifier)
                        );
        }
        return dataEnc;
    }

    // private List<String> process(List<?> data) {
    //     if(data == null) {
    //         return null;
    //     }
    //     return data.stream().map( 
    //             e -> encryptData(e,new FieldMetaData(
    //                     CIPHER_ALGORITHM_DEFAULT,
    //                     Optional.ofNullable(e).map(o -> o.getClass().getName()).orElse(""),
    //                     defaultCipherDataKeyIdentifier
    //                 )
    //             )
    //         ).collect(Collectors.toList());
    // }

    //explicit typeinference somehow hinders "autoboxing" of array types e.g. int[] vs Integer[]
    // @Override
	// public TypeInference getTypeInference(DataTypeFactory typeFactory) {
	// 	return TypeInference.newBuilder()
    //             // .inputTypeStrategy(InputTypeStrategies.sequence(
    //             //         Collections.singletonList("data"),
    //             //         Collections.singletonList(InputTypeStrategies.logical(LogicalTypeRoot.ARRAY))
    //             //     ))
    //             .inputTypeStrategy(
    //                     InputTypeStrategies.sequence(
    //                             InputTypeStrategies.ANY // SHOULD BE LIMITED to "any array" i.e. ARRAY<T>
    //                     ))
    //             .outputTypeStrategy(TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.STRING())))
	// 			.build();
	// }

}
