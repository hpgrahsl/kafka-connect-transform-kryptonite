package com.github.hpgrahsl.flink.functions.kryptonite;

import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.flink.table.functions.FunctionContext;
import com.github.hpgrahsl.kryptonite.FieldMetaData;
import com.github.hpgrahsl.kryptonite.config.KryptoniteSettings;

public class EncryptArrayUdf extends AbstractCipherFieldUdf {

    private transient String defaultCipherDataKeyIdentifier;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        defaultCipherDataKeyIdentifier = context.getJobParameter(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER,KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT);
        if(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT.equals(defaultCipherDataKeyIdentifier)) {
            defaultCipherDataKeyIdentifier = Optional.ofNullable(System.getenv(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER)).orElse(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT);
        }
    }

    public @Nullable String[] eval(@Nullable final String[] data) {
        return process(data,defaultCipherDataKeyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    // public @Nullable String[] eval(@Nullable final String[] data, String keyIdentifier) {
    //     return process(data,keyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    // }

    public @Nullable String[] eval(@Nullable final String[] data, String keyIdentifier, String cipherAlgorithm) {
        return process(data,keyIdentifier,cipherAlgorithm);
    }

    public @Nullable String[] eval(@Nullable final Boolean[] data) {
        return process(data,defaultCipherDataKeyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    // public @Nullable String[] eval(@Nullable final Boolean[] data, String keyIdentifier) {
    //     return process(data,keyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    // }

    public @Nullable String[] eval(@Nullable final Boolean[] data, String keyIdentifier, String cipherAlgorithm) {
        return process(data,keyIdentifier,cipherAlgorithm);
    }

    public @Nullable String[] eval(@Nullable final Integer[] data) {
        return process(data,defaultCipherDataKeyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    // public @Nullable String[] eval(@Nullable final Integer[] data, String keyIdentifier) {
    //     return process(data,keyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    // }

    public @Nullable String[] eval(@Nullable final Integer[] data, String keyIdentifier, String cipherAlgorithm) {
        return process(data,keyIdentifier,cipherAlgorithm);
    }

    public @Nullable String[] eval(@Nullable final Long[] data) {
        return process(data,defaultCipherDataKeyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    // public @Nullable String[] eval(@Nullable final Long[] data, String keyIdentifier) {
    //     return process(data,keyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    // }

    public @Nullable String[] eval(@Nullable final Long[] data, String keyIdentifier, String cipherAlgorithm) {
        return process(data,keyIdentifier,cipherAlgorithm);
    }

    public @Nullable String[] eval(@Nullable final Float[] data) {
        return process(data,defaultCipherDataKeyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    // public @Nullable String[] eval(@Nullable final Float[] data, String keyIdentifier) {
    //     return process(data,keyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    // }

    public @Nullable String[] eval(@Nullable final Float[] data, String keyIdentifier, String cipherAlgorithm) {
        return process(data,keyIdentifier,cipherAlgorithm);
    }

    public @Nullable String[] eval(@Nullable final Double[] data) {
        return process(data,defaultCipherDataKeyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    // public @Nullable String[] eval(@Nullable final Double[] data, String keyIdentifier) {
    //     return process(data,keyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    // }

    public @Nullable String[] eval(@Nullable final Double[] data, String keyIdentifier, String cipherAlgorithm) {
        return process(data,keyIdentifier,cipherAlgorithm);
    }

    // NOTE: for non nullable array of var/binary
    // -> otherwise should be wrapper type Byte[][]
    // public @Nullable String[] eval(final byte[][] data) {
    //     return process(data,defaultCipherDataKeyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    // }

    // public @Nullable String[] eval(final byte[][] data, String keyIdentifier) {
    //     return process(data,keyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    // }

    // public @Nullable String[] eval(final byte[][] data, String keyIdentifier, String cipherAlgorithm) {
    //     return process(data,keyIdentifier,cipherAlgorithm);
    // }

    private <T> String[] process(T[] array, String keyIdentifier, String cipherAlgorithm) {
        if(array == null ) {
            return null;
        }
        var arrayEnc = new String[array.length];
        for(int s = 0; s < array.length; s++) {
            arrayEnc[s] = encryptData(
                            array[s],
                            new FieldMetaData(
                                cipherAlgorithm,
                                Optional.ofNullable(array[s]).map(o -> o.getClass().getName()).orElse(""),
                                keyIdentifier
                            )
                        );
        }
        return arrayEnc;
    }

}
