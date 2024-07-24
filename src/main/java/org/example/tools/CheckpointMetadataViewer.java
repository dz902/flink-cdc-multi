package org.example.tools;

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.*;
import java.io.DataInputStream;
import java.io.FileInputStream;
import static org.apache.flink.runtime.checkpoint.Checkpoints.HEADER_MAGIC_NUMBER;

// COURTESY: https://ngela.tistory.com/49

public class CheckpointMetadataViewer {
    public static void main(String[] args) throws Exception {

        String metaPath = "/private/tmp/test/checkpoints/57bef2969afeda3fa87f90451cc766d4/chk-2/_metadata";
        FileInputStream fis = new FileInputStream(metaPath);
        DataInputStream dis = new DataInputStream(fis);

        // header_magic_number + version + [data...]
        int headerMagicNumber = dis.readInt();
        if (headerMagicNumber != HEADER_MAGIC_NUMBER) {
            throw new Exception(
                String.format(">>> [CHECKPOINT-METADATA-VIEWER] UNRECOGNIZED MAGIC NUMBER: %d", headerMagicNumber)
            );
        }

        int version = dis.readInt();
        MetadataSerializer serializer = MetadataSerializers.getSerializer(version);

        CheckpointMetadata meta = serializer.deserialize(dis,
            CheckpointMetadata.class.getClassLoader(),
            null
        );

        System.out.println("getCheckpointId()          : " + meta.getCheckpointId());
        System.out.println("getMasterStates().size()   : " + meta.getMasterStates().size());
        int idx = 0;
        for(MasterState masterState : meta.getMasterStates()) {
            System.out.println("["+idx+"]" + masterState);
            idx+=1;
        }
        System.out.println("getOperatorStates().size() : " + meta.getOperatorStates().size());
        idx = 0;
        for(OperatorState operatorState : meta.getOperatorStates()) {
            System.out.println("["+idx+"].operatorState       : " + operatorState);
            idx+=1;
        }
        dis.close();
        fis.close();
    }
}