import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * @author Akshay Darshan Arnab Aniruddha
 *
 */
public class Process implements NetworkInterface {

	// Gson
	private Gson mGson;

	private AbstractProcess mSelf;

	public int mProcessId;
	private NetworkWrapper mNetworkWrapper;
	// private PriorityBlockingQueue<AbstractProcess> mProcessRequestQueue = new
	// PriorityBlockingQueue<AbstractProcess>();
	private PriorityQueue<AbstractProcess> mProcessRequestQueue = new PriorityQueue<AbstractProcess>();
	// Indicates if current process is locked for other process
	private volatile boolean isLocked = false;

	// Indicates if current process has ever received a failed message
	private volatile boolean hasReceivedFailedMessage = false;

	// Indicates if current process has ever received an inquire message
	private volatile boolean hasReceivedInquireMessage = false;

	/**
	 * This process will be used in case of inquire message. When an inquire
	 * message is received, this process will indicate which process has sent an
	 * inquire message. In future when a failed message is received, a
	 * relinquish message will be sent to this process
	 */
	private AbstractProcess mRelinquishDestination = null;

	private int mIncomingLockCounter;
	private boolean mReceivedPermission;
	private boolean mPermissionFlag;
	private String mControllerIp;
	private int mControllerPort;

	// HashMaps
	private HashMap<Integer, String> mProcessIpAddresses;
	private HashMap<Integer, Integer> mProcessPorts;

	public Process(int processid) {
		this.mProcessId = processid;
		addShutdownHook(this);
		// Init server
		this.mNetworkWrapper = NetworkWrapper.getInstance(this);
		isLocked = false;
		mReceivedPermission = false;

		mPermissionFlag = false;
		this.mProcessIpAddresses = new HashMap<Integer, String>();
		this.mProcessPorts = new HashMap<Integer, Integer>();
		this.initProcesses();
		this.mGson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
				.create();
		this.mSelf = new AbstractProcess(this.mProcessId, 0);
	}

	private void parseConfigFile() {
		File configFile = new File("process.config");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(configFile));
			String curr;
			while ((curr = reader.readLine()) != null) {
				String splitCurr[] = curr.split("\t");
				int id = Integer.parseInt(splitCurr[0]);
				if (id == this.mProcessId && splitCurr.length == 1) {
					this.readSelf(reader);
					return;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void readSelf(BufferedReader reader) throws IOException {
		String curr;
		boolean hasReadController = false;
		while (true) {
			curr = reader.readLine();
			if (curr == null)
				return;
			String splitCurr[] = curr.split("\t");
			if (splitCurr.length == 1)
				return;
			if (!hasReadController) {
				this.mControllerIp = splitCurr[1];
				this.mControllerPort = Integer.parseInt(splitCurr[2]);
				hasReadController = true;
			} else {
				int pid = Integer.parseInt(splitCurr[0]);
				this.mProcessIpAddresses.put(pid, splitCurr[1]);
				this.mProcessPorts.put(pid, Integer.parseInt(splitCurr[2]));
			}
		}
	}

	/**
	 * Basic init operations
	 */
	private void initProcesses() {
		this.parseConfigFile();
	}

	// Function that initiates execution of critical section
	private void executeCriticalSection() {
		// TODO code changed
		this.isLocked = true;
		this.sendRequestMessage();
		this.sendPermissionRequestMessage();
	}

	private void sendPermissionRequestMessage() {

		JSONObject object = new JSONObject();

		try {
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_REQUEST_INTERNAL);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			// TODO set appropriate size()
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mControllerIp, this.mControllerPort);
			System.out.println("Sent request message to controller");
		} catch (JSONException e) {
			e.printStackTrace();
			return;
		}

	}

	private synchronized void sendRequestMessage() {
		String ip;
		int port;
		int clockValue = LamportClock.getClockValue();
		this.mSelf.setTimestamp(clockValue);
		JSONObject object;
		this.mProcessRequestQueue.add(new AbstractProcess(this.mProcessId,
				clockValue));
		Set<Integer> keys = this.mProcessIpAddresses.keySet();

		for (Integer i : keys) {
			if (i == this.mProcessId)
				continue;
			ip = this.mProcessIpAddresses.get(i);
			port = this.mProcessPorts.get(i);
			object = new JSONObject();
			try {
				object.put(Constants.MESSAGE_TYPE, Constants.TYPE_REQUEST);
				object.put(Constants.MESSAGE_INFO,
						new JSONObject(this.mGson.toJson(this.mSelf)));
				// TODO set appropriate size()
				this.mIncomingLockCounter = this.mProcessIpAddresses.size() - 1;
				this.mNetworkWrapper.sendMessage(object.toString(), ip, port);
				System.out.println("Sent request message to : " + i);
			} catch (JSONException e) {
				e.printStackTrace();
				return;
			}
		}
	}

	public static void main(String args[]) throws Exception {
		int processId = Integer.parseInt(args[0]);
		Process process = new Process(processId);
		Thread.sleep(5000);
		process.executeCriticalSection();
		Scanner scanner = new Scanner(System.in);
		String input = scanner.nextLine();
		if (input.equalsIgnoreCase("end"))
			System.exit(0);
		scanner.close();
	}

	public static void addShutdownHook(final Process p) {
		Runtime runtime = Runtime.getRuntime();
		runtime.addShutdownHook(new Thread() {
			@Override
			public void run() {
				NetworkWrapper.getInstance(p).closeServer();
			}
		});
	}

	@Override
	public synchronized void messageReceived(String message) {
		JSONObject object = null;
		try {
			object = new JSONObject(message);
			switch (object.getString(Constants.MESSAGE_TYPE)) {

			// Incoming locked message
			case Constants.TYPE_LOCK:
				this.mIncomingLockCounter--;
				this.checkForCriticalExecution(object);
				break;

			// Incoming request message by other process
			case Constants.TYPE_REQUEST:
				this.processIncomingRequest(object);
				break;

			// Incoming failed message
			case Constants.TYPE_FAILED:
				this.processFailedMessage(object);
				break;

			// Incoming inquire message
			case Constants.TYPE_INQUIRE:
				this.processInquireMessage(object);
				break;

			// Incoming relinquish message
			case Constants.TYPE_RELINQUISH:
				this.processRelinquishMessage(object);
				break;

			// Incoming release message
			case Constants.TYPE_RELEASE:
				this.processReleaseMessage(object);
				break;

			// Incoming permission message from controller
			case Constants.TYPE_PERMISSION:
				this.processPermission(object);
				break;

			}
		} catch (JSONException e) {
			e.printStackTrace();
			System.out.println("Gandla");
		}
	}

	private void processPermission(JSONObject object) {
		this.mReceivedPermission = true;
		this.mPermissionFlag = true;
		checkForCriticalExecution(object);

	}

	private void processReleaseMessage(JSONObject object) {
		int toRemovePid;
		try {
			toRemovePid = object.getJSONObject(Constants.MESSAGE_INFO).getInt(
					Constants.PROCESS_ID);
			AbstractProcess toRemove = new AbstractProcess(toRemovePid, -1);
			this.mProcessRequestQueue.remove(toRemove);

			this.isLocked = false;
			this.processNext();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processNext() {
		if (!this.mProcessRequestQueue.isEmpty()) {
			if (!this.mProcessRequestQueue.peek().equals(this.mSelf))
				this.sendLockedMessage(this.mProcessRequestQueue.peek());
		}
	}

	private void resetFlags() {
		this.isLocked = false;
		this.hasReceivedFailedMessage = false;
		this.hasReceivedInquireMessage = false;
		this.mRelinquishDestination = null;
	}

	private void processRelinquishMessage(JSONObject object) {
		JSONObject lockMessage = new JSONObject();
		try {
			lockMessage.put(Constants.MESSAGE_TYPE, Constants.TYPE_LOCK);
			lockMessage.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			int toSendLockPid = this.mProcessRequestQueue.peek().getPID();
			this.mNetworkWrapper.sendMessage(lockMessage.toString(),
					this.mProcessIpAddresses.get(toSendLockPid),
					this.mProcessPorts.get(toSendLockPid));
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processInquireMessage(JSONObject object) {
		int pid;
		try {
			pid = object.getJSONObject(Constants.MESSAGE_INFO).getInt(
					Constants.PROCESS_ID);
			AbstractProcess otherProcess = new AbstractProcess(pid, -1);
			this.mRelinquishDestination = otherProcess;
			if (this.hasReceivedFailedMessage) {
				this.sendRelinquishMessage(otherProcess);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processFailedMessage(JSONObject object) {
		try {
			this.hasReceivedFailedMessage = true;
			if (this.hasReceivedInquireMessage) {
				// There was a previous inquire message received, now send a
				// relinquish message
				this.sendRelinquishMessage(this.mRelinquishDestination);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sendRelinquishMessage(AbstractProcess other) {
		JSONObject object = new JSONObject();
		try {
			this.mIncomingLockCounter++;
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_RELINQUISH);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(other.getPID()),
					this.mProcessPorts.get(other.getPID()));
			System.out
					.println("Sent relinquish message to : " + other.getPID());
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processIncomingRequest(JSONObject object) {
		AbstractProcess topProcess = this.mProcessRequestQueue.peek();
		AbstractProcess incomingProcess;
		try {
			incomingProcess = new AbstractProcess(object.getJSONObject(
					Constants.MESSAGE_INFO).getInt("pid"), object
					.getJSONObject(Constants.MESSAGE_INFO).getInt("timestamp"));
			this.mProcessRequestQueue.add(incomingProcess);
			if (topProcess != null
					&& !topProcess.equals(this.mProcessRequestQueue.peek())
					&& this.isLocked) {
				// Conditions to check before sending inquire message
				// 1. isLocked = true i.e current process is locked for other
				// process
				// 2. queue after adding process to queue, compare if most
				// recent process has changed

				this.sendInquireMessage(topProcess);
			} else if (this.isLocked) {
				this.sendFailedMessage(incomingProcess);
			} else {
				this.sendLockedMessage(incomingProcess);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void sendLockedMessage(AbstractProcess incomingProcess) {

		JSONObject object = new JSONObject();
		try {
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_LOCK);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(incomingProcess.getPID()),
					this.mProcessPorts.get(incomingProcess.getPID()));
			this.isLocked = true;
			System.out.println("Sent locked message to : "
					+ incomingProcess.getPID());
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void sendFailedMessage(AbstractProcess incomingProcess) {

		JSONObject object = new JSONObject();
		try {
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_FAILED);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(incomingProcess.getPID()),
					this.mProcessPorts.get(incomingProcess.getPID()));
			System.out.println("Sent failed message to : "
					+ incomingProcess.getPID());
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void sendInquireMessage(AbstractProcess incomingProcess) {
		System.out.println("Sending inquire: " + incomingProcess.getPID());
		JSONObject object = new JSONObject();
		try {
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_INQUIRE);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(incomingProcess.getPID()),
					this.mProcessPorts.get(incomingProcess.getPID()));
			System.out.println("Sent inquire message to : "
					+ incomingProcess.getPID());
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void checkForCriticalExecution(JSONObject object) {
		// if(!this.mPermissionFlag)
		// this.mIncomingLockCounter--;

		System.out.println("Lock counter:" + this.mIncomingLockCounter);
		if (this.mIncomingLockCounter == 0 && this.mReceivedPermission) {
			// Received Lock from all processes of quorums
			try {
				this.criticalSection();
				this.sendReleaseMessage();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void sendReleaseMessage() {
		JSONObject object;
		Set<Integer> keys = this.mProcessIpAddresses.keySet();

		// for (int i = 1; i <= this.mProcessIpAddresses.size(); i++) {
		for (Integer i : keys) {
			if (i == this.mSelf.getPID())
				continue;
			object = new JSONObject();
			try {
				object.put(Constants.MESSAGE_TYPE, Constants.TYPE_RELEASE);
				// TODO dummy value
				object.put(Constants.MESSAGE_INFO,
						new JSONObject(this.mGson.toJson(this.mSelf)));
				this.mNetworkWrapper.sendMessage(object.toString(),
						this.mProcessIpAddresses.get(i),
						this.mProcessPorts.get(i));
				System.out.println("Sent release message to : " + i);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			object = new JSONObject();
			object.put(Constants.MESSAGE_TYPE,
					Constants.TYPE_EXIT_CRITICAL_SECTION);
			// TODO dummy value
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mControllerIp, this.mControllerPort);

		} catch (Exception e) {
			e.printStackTrace();
		}
		this.resetFlags();
		if (this.mProcessRequestQueue.remove(this.mSelf))
			System.out.println("Removed from queue: " + this.mSelf.getPID());
		else
			System.out.println("There was an error removing process: "
					+ this.mSelf.getPID() + " from process queue");
		this.processNext();
	}

	private void criticalSection() throws InterruptedException {
		SimpleDateFormat format = new SimpleDateFormat("hh:mm:ss");
		System.out.println("Process: " + this.mProcessId
				+ " entering critical section: "
				+ format.format(new Date(System.currentTimeMillis())));
		Thread.sleep(10000);
		System.out.println("Process: " + this.mProcessId
				+ " exiting critical section: "
				+ format.format(new Date(System.currentTimeMillis()))
				+ "\n\n\n");
	}
}

class LamportClock {
	private static int clock = 0;

	private static void tick() {
		clock++;
	}

	public static int getClockValue() {
		tick();
		return clock;
	}
}

class AbstractProcess implements Comparable<AbstractProcess> {

	@Expose
	private int pid, timestamp;

	public AbstractProcess(int pid, int timestamp, String ip, int port) {
		this.pid = pid;
		this.timestamp = timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

	public int getPID() {
		return this.pid;
	}

	public AbstractProcess(int pid, int timestamp) {
		this.pid = pid;
		this.timestamp = timestamp;
	}

	@Override
	public int compareTo(AbstractProcess o) {
		if (this.timestamp < o.timestamp) {
			return -1;
		} else if (this.timestamp > o.timestamp) {
			return 1;
		} else {
			if (this.pid < o.pid)
				return -1;
			else
				return 1;
		}
	}

	@Override
	public boolean equals(Object obj) {
		AbstractProcess other = (AbstractProcess) obj;
		return other.getPID() == this.pid;
	}
}
