import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;
import java.nio.charset.StandardCharsets;
//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;


public class WorkerHandler implements AsyncCallback.DataCallback {
    public String zkhost;
    public ZooKeeper zk;
    public WorkerHandler(String zkhost, ZooKeeper zk) {
        super();
        this.zk = zk;
        this.zkhost = zkhost;
    }

    // handle worker stuff
    public void processResult(int rc, java.lang.String path, java.lang.Object ctx, byte[] data, Stat stat){
        try {
            //its def a worker thing
            String workerName = "worker-" + zkhost;
            String status = getStringFromData(data);
            if(status.equals("idle")) {
                return;
            }

            String task = status;

            // we have some work to do
            //TODO!! This is not a good approach, you should get the data using an async version of the API.
            byte[] taskSerial = zk.getData("/dist31/tasks/"+task, false, null);

            // Re-construct our task object.
            ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
            ObjectInput in = new ObjectInputStream(bis);
            DistTask dt = (DistTask) in.readObject();

            //Execute the task.
            //TODO: Again, time consuming stuff. Should be done by some other thread and not inside a callback!
            dt.compute();
            
            // Serialize our Task object back to a byte array!
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(dt); oos.flush();
            taskSerial = bos.toByteArray();

            // Store it inside the result node.
            zk.create("/dist31/tasks/"+task+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //zk.create("/dist31/tasks/"+c+"/result", ("Hello from "+pinfo).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            
            // we need to write idle
            zk.setData("/dist31/workers/" + workerName, "idle".getBytes(), -1);
        }
		catch(Exception ke){
            System.out.println(ke);
        }
    }

    private String getStringFromData(byte [] data) {
        // Re-construct our worker status.
		return new String(data, StandardCharsets.UTF_8);
	}
}
