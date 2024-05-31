package com.github.hpgrahsl.flink.functions.kryptonite;

import java.io.ByteArrayOutputStream;
import java.util.Base64;
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
        //TODO: load key config via the function context
        var cipherDataKeysConfig = "["
            + "{\"identifier\":\"keyA\","
            + "\"material\":{"
            + "\"primaryKeyId\":1000000001,"
            + "\"key\":["
            + "{\"keyData\":"
            + "{\"typeUrl\":\"type.googleapis.com/google.crypto.tink.AesGcmKey\","
            + "\"value\":\"GhDRulECKAC8/19NMXDjeCjK\","
            + "\"keyMaterialType\":\"SYMMETRIC\"},"
            + "\"status\":\"ENABLED\","
            + "\"keyId\":1000000001,"
            + "\"outputPrefixType\":\"TINK\""
            + "}"
            + "]"
            + "}"
            + "},"
            + "{\"identifier\":\"keyB\","
            + "\"material\":{"
            + "\"primaryKeyId\":1000000002,"
            + "\"key\":["
            + "{\"keyData\":"
            + "{\"typeUrl\":\"type.googleapis.com/google.crypto.tink.AesGcmKey\","
            + "\"value\":\"GiBIZWxsbyFXb3JsZEZVQ0sxYWJjZGprbCQxMjM0NTY3OA==\","
            + "\"keyMaterialType\":\"SYMMETRIC\"},"
            + "\"status\":\"ENABLED\","
            + "\"keyId\":1000000002,"
            + "\"outputPrefixType\":\"TINK\""
            + "}"
            + "]"
            + "}"
            + "},"
            + "{\"identifier\":\"key9\","
            + "\"material\":{"
            + "\"primaryKeyId\":1000000003,"
            + "\"key\":["
            + "{\"keyData\":"
            + "{\"typeUrl\":\"type.googleapis.com/google.crypto.tink.AesSivKey\","
            + "\"value\":\"EkByiHi3H9shy2FO5UWgStNMmgqF629esenhnm0wZZArUkEU1/9l9J3ajJQI0GxDwzM1WFZK587W0xVB8KK4dqnz\","
            + "\"keyMaterialType\":\"SYMMETRIC\"},"
            + "\"status\":\"ENABLED\","
            + "\"keyId\":1000000003,"
            + "\"outputPrefixType\":\"TINK\""
            + "}"
            + "]"
            + "}"
            + "},"
            + "{\"identifier\":\"key8\","
            + "\"material\":{"
            + "\"primaryKeyId\":1000000004,"
            + "\"key\":["
            + "{\"keyData\":"
            + "{\"typeUrl\":\"type.googleapis.com/google.crypto.tink.AesSivKey\","
            + "\"value\":\"EkBWT3ZL7DmAN91erW3xAzMFDWMaQx34Su3VlaMiTWzjVDbKsH3IRr2HQFnaMvvVz2RH/+eYXn3zvAzWJbReCto/\","
            + "\"keyMaterialType\":\"SYMMETRIC\"},"
            + "\"status\":\"ENABLED\","
            + "\"keyId\":1000000004,"
            + "\"outputPrefixType\":\"TINK\""
            + "}"
            + "]"
            + "}"
            + "}"
            + "]";
        try {
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
