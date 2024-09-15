package com.github.hpgrahsl.flink.functions.kryptonite;

import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.hpgrahsl.kryptonite.EncryptedField;
import com.github.hpgrahsl.kryptonite.FieldMetaData;
import com.github.hpgrahsl.kryptonite.Kryptonite;
import com.github.hpgrahsl.kryptonite.PayloadMetaData;
import com.github.hpgrahsl.kryptonite.config.DataKeyConfig;
import com.github.hpgrahsl.kryptonite.config.KryptoniteSettings;
import com.github.hpgrahsl.kryptonite.keys.TinkKeyVault;
import com.github.hpgrahsl.kryptonite.serdes.KryoInstance;
import com.github.hpgrahsl.kryptonite.serdes.KryoSerdeProcessor;
import com.github.hpgrahsl.kryptonite.serdes.SerdeProcessor;

public abstract class AbstractCipherFieldUdf extends ScalarFunction {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private transient Kryptonite kryptonite;
    private transient SerdeProcessor serdeProcessor;

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            var cipherDataKeysConfig = context.getJobParameter(
                KryptoniteSettings.CIPHER_DATA_KEYS,
                KryptoniteSettings.CIPHER_DATA_KEYS_DEFAULT
            );
            if(KryptoniteSettings.CIPHER_DATA_KEYS_DEFAULT.equals(cipherDataKeysConfig)) {
                cipherDataKeysConfig = Optional.ofNullable(System.getenv(KryptoniteSettings.CIPHER_DATA_KEYS)).orElse(KryptoniteSettings.CIPHER_DATA_KEYS_DEFAULT);
            }
            var dataKeyConfig = OBJECT_MAPPER.readValue(
                    cipherDataKeysConfig,
                    new TypeReference<Set<DataKeyConfig>>() {}
            );
            var keyConfigs = dataKeyConfig.stream().collect(
                    Collectors.toMap(DataKeyConfig::getIdentifier, DataKeyConfig::getMaterial));
            kryptonite = new Kryptonite(new TinkKeyVault(keyConfigs));
            serdeProcessor = new KryoSerdeProcessor();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    String encryptData(Object data, FieldMetaData fieldMetaData) {
        try {
            var valueBytes = serdeProcessor.objectToBytes(data);
            var encryptedField = kryptonite.cipherField(valueBytes, PayloadMetaData.from(fieldMetaData));
            var output = new Output(new ByteArrayOutputStream());
            KryoInstance.get().writeObject(output, encryptedField);
            var encodedField = Base64.getEncoder().encodeToString(output.toBytes());
            return encodedField;
        } catch (Exception exc) {
            exc.printStackTrace();
        }
        return null;
    }

    Object decryptData(String data) {
        try {
            var encryptedField = KryoInstance.get().readObject(new Input(Base64.getDecoder().decode(data)), EncryptedField.class);
            var plaintext = kryptonite.decipherField(encryptedField);
            var restored = serdeProcessor.bytesToObject(plaintext);
            return restored;
        } catch (Exception exc) {
            exc.printStackTrace();
        }
        return null;
    }

}
