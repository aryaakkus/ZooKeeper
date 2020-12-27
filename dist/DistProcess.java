import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;
import java.nio.charset.StandardCharsets;
//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watch ges & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback {
	ZooKeeper zk;
	String zkServer, pinfo, hostname;
	boolean isMaster = false;
	List<String> workers = null; // only used by master
	WorkerHandler workerHandler = null; // worker handler
	List<String> assignedTasks = new ArrayList<String>();

	DistProcess(String zkhost) {
		zkServer = zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		hostname = pinfo.split("@")[1];
		System.out.println("DISTAPP31 : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP31 : Process information : " + pinfo);
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
		zk = new ZooKeeper(zkServer, 1000, this); // connect to ZK.
		this.workerHandler = new WorkerHandler(hostname, zk);
		try {
			runForMaster(); // See if you can become the master (i.e, no other master exists)
			isMaster = true;
			getTasks(); // Install monitoring on any new tasks that will be created.
			getWorkers(); // Install monitor worker nodes from Master
		} catch (NodeExistsException nee) {
			isMaster = false;
			installWorker();
			workerSearchingForWork(); // Install monitor for tasks for this worker
		}

		System.out.println("DISTAPP : Role : " + " I will be functioning as " + (isMaster ? "master" : "worker"));
	}

	// Install worker
	void installWorker() {
		try {
			zk.create("/dist31/workers/worker-" + hostname, "idle".getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
		} catch (Exception e) {
			System.out.print(e);
		}
	}

	// Master fetching task znodes...
	void getTasks() {
		zk.getChildren("/dist31/tasks", this, this, null);
	}

	void workerSearchingForWork() {
		zk.getData("/dist31/workers/worker-" + hostname, this, this.workerHandler, null);
	}

	// Master fetching the worker znodes...
	void getWorkers() {
		zk.getChildren("/dist31/workers", this, this, null);
	}

	// Try to become the master.
	void runForMaster() throws UnknownHostException, KeeperException, InterruptedException {
		// Try to create an ephemeral node to be the master, put the hostname and pid of
		// this process as the data.
		// This is an example of Synchronous API invocation as the function waits for
		// the execution and no callback is involved..
		try {
			zk.create("/dist31", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
			// ignore
			System.out.println("DEBUG:: /dist31 already exsits!!!");
		}

		try {
			zk.create("/dist31/workers", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
			// ignore
			System.out.println("DEBUG:: /dist31/workers already exsits!!!");
		}

		try {
			zk.create("/dist31/tasks", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
			// ignore
			System.out.println("DEBUG:: /dist31/tasks already exsits!!!");
		}

		zk.create("/dist31/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	// watcher
	public void process(WatchedEvent e) {
		// Get watcher notifications.

		// !! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		// including in other functions called from here.
		// Your will be essentially holding up ZK client library
		// thread and you will not get other notifications.
		// Instead include another thread in your program logic that
		// does the time consuming "work" and notify that thread from here.

		System.out.println("DISTAPP : Event received : " + e);
		// Master should be notified if any new znodes are added to tasks.
		if (isMaster && e.getType() == Watcher.Event.EventType.NodeChildrenChanged
				&& e.getPath().equals("/dist31/tasks")) {
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the
			// children.
			getTasks();
		} else if (isMaster && e.getType() == Watcher.Event.EventType.NodeChildrenChanged
				&& e.getPath().equals("/dist31/workers")) {
			getWorkers();
		} else if (!isMaster && e.getType() == Watcher.Event.EventType.NodeDataChanged
				&& e.getPath().equals("/dist31/workers/worker-" + hostname)) {
			workerSearchingForWork();
		}
	}

	// helper method for converting byte array to String
	private String getStringFromData(byte[] data) {
		return new String(data, StandardCharsets.UTF_8);
	}

	// check if worker is available
	public boolean workerAvailable(String workerName) {
		try {
			byte[] data = zk.getData("/dist31/workers/" + workerName, false, null);
			return getStringFromData(data).equals("idle");
		} catch (Exception e) {
			System.out.println(e);
		}
		return false;
	}

	// assign a task to a worker
	public void assignTask(String workerName, String taskName) {
		try {
			zk.setData("/dist31/workers/" + workerName, taskName.getBytes(), -1);
			assignedTasks.add(taskName);
		} catch (Exception ke) {
			System.out.println(ke);
		}
	}

	// handler for addition of workers
	public void workerHandlerers(int rc, Object ctx, List<String> workers) {
		this.workers = workers;
	}

	// handler for addition of tasks
	public void handleTasks(int rc, Object ctx, List<String> children) {
		if (children == null || children.size() == 0)
			return;
		for (String task : children) {
			if(assignedTasks.contains(task)){
				System.out.println("task " + task + " was culled because it is already assigned");
				continue;
			}
			System.out.println(task);
			try {
				// that should be moved done by a process function as the worker.
				boolean foundWorker = false;
				while(!foundWorker) {
					List<String> workers = zk.getChildren("/dist31/workers", false);
					for (String workerName : workers) {
						if (workerAvailable(workerName)) {
							assignTask(workerName, task);
							foundWorker = true;
							System.out.println("worker " + workerName + " was assigned task " + task);
							break;
						}
					}

					if(!foundWorker) {
						Thread.sleep(1000);
					}
				}
				
			} catch (Exception nee) {
				System.out.println(nee);
			}
		}
	}

	// handler: Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children) {

		// !! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		// including in other functions called from here.
		// Your will be essentially holding up ZK client library
		// thread and you will not get other notifications.
		// Instead include another thread in your program logic that
		// does the time consuming "work" and notify that thread from here.

		// This logic is for master !!
		// Every time a new task znode is created by the client, this will be invoked.

		// TODO: Filter out and go over only the newly created task znodes.
		// Also have a mechanism to assign these tasks to a "Worker" process.
		// The worker must invoke the "compute" function of the Task send by the client.
		// What to do if you do not have a free worker process?
		System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
		if (path.contains("tasks") && isMaster) {
			handleTasks(rc, ctx, children);
		} else if (path.contains("worker") && isMaster) {
			workerHandlerers(rc, ctx, children);
		}
	}

	public static void main(String args[]) throws Exception {
		// Create a new process
		// Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		// Replace this with an approach that will make sure that the process is up and
		// running forever.
		while(true) {
			Thread.sleep(1000);
		}
	}
}
