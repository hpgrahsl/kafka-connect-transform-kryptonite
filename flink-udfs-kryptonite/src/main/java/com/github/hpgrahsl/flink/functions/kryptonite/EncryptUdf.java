package com.github.hpgrahsl.flink.functions.kryptonite;

import java.util.Optional;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.FunctionContext;

import com.github.hpgrahsl.kryptonite.FieldMetaData;
import com.github.hpgrahsl.kryptonite.crypto.tink.TinkAesGcm;

public class EncryptUdf extends AbstractCipherFieldUdf {

    private final static String CIPHER_ALGORITHM_DEFAULT = TinkAesGcm.CIPHER_ALGORITHM;
    
    private transient String defaultCipherDataKeyIdentifier;
    
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        //TODO: load default key id via the function context
        defaultCipherDataKeyIdentifier = "keyA";
    }

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) final Object data) {
        var fmd = new FieldMetaData(
            CIPHER_ALGORITHM_DEFAULT,
            Optional.ofNullable(data).map(o -> o.getClass().getName()).orElse(""),
            defaultCipherDataKeyIdentifier
        );
        return encryptData(data,fmd);
    }

}
