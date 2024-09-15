package com.github.hpgrahsl.flink.functions.kryptonite;

import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.FunctionContext;

import com.github.hpgrahsl.kryptonite.FieldMetaData;
import com.github.hpgrahsl.kryptonite.config.KryptoniteSettings;

public class EncryptUdf extends AbstractCipherFieldUdf {
    
    private transient String defaultCipherDataKeyIdentifier;
    
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        defaultCipherDataKeyIdentifier = context.getJobParameter(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER,KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT);
        if(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT.equals(defaultCipherDataKeyIdentifier)) {
            defaultCipherDataKeyIdentifier = Optional.ofNullable(System.getenv(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER)).orElse(KryptoniteSettings.CIPHER_DATA_KEY_IDENTIFIER_DEFAULT);
        }        
    }

    /**
     * Encrypt primitive or complex field data in object mode
     * using the configured defaults for key identifier and cipher algorithm
     * 
     * @param data the data to encrypt
     * @return BASE64 encoded ciphertext
     */
    public @Nullable String eval(@Nullable @DataTypeHint(inputGroup = InputGroup.ANY) final Object data) {
        return eval(data,defaultCipherDataKeyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    /**
     * Encrypt primitive or complex field data in object mode
     * using the specified key identifier and the configured default cipher algorithm
     * 
     * @param data the data to encrypt
     * @param keyIdentifier the key identifier for secret key material to use
     * @return BASE64 encoded ciphertext
     */
    public @Nullable String eval(@Nullable @DataTypeHint(inputGroup = InputGroup.ANY) final Object data, String keyIdentifier) {
        return eval(data,keyIdentifier,KryptoniteSettings.CIPHER_ALGORITHM_DEFAULT);
    }

    /**
     * Encrypt primitive or complex field data in object mode
     * using the specified key identifier and cipher algorithm
     * 
     * @param data the data to encrypt
     * @param keyIdentifier the key identifier for the secret key material to use
     * @param cipherAlgorithm the cipher algorithm to apply
     * @return BASE64 encoded ciphertext
     */
    public @Nullable String eval(@Nullable @DataTypeHint(inputGroup = InputGroup.ANY) final Object data, String keyIdentifier, String cipherAlgorithm) {
        var fmd = new FieldMetaData(
            cipherAlgorithm,
            Optional.ofNullable(data).map(o -> o.getClass().getName()).orElse(""),
            keyIdentifier
        );
        return encryptData(data,fmd);
    }

}
