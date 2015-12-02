package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import sun.plugin2.message.Message;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Created by Roman on 12/1/2015.
 */
public class TraversalHashIdProvider implements TraversalIdProvider<String> {
    //region Constructor
    public TraversalHashIdProvider(TraversalIdProvider<String> innerIdProvider, String hashAlgorithm) throws NoSuchAlgorithmException {
        this.innerIdProvider = innerIdProvider;
        this.md = MessageDigest.getInstance(hashAlgorithm);
        this.encoder = Base64.getEncoder();
    }
    //endregion

    //region TraversalIdProvider Implementation
    @Override
    public String getId(Traversal traversal) {
        String traversalId = this.innerIdProvider.getId(traversal);
        try {
            byte[] traversalIdBytes = traversalId.getBytes("UTF-8");
            byte[] hashBytes = md.digest(traversalIdBytes);
            return this.encoder.encodeToString(hashBytes);
        } catch(UnsupportedEncodingException ex) {
            // ???
        }

        return null;
    }
    //endregion

    //region Fields
    private TraversalIdProvider<String> innerIdProvider;
    private MessageDigest md;
    private Base64.Encoder encoder;
    //endregion
}
