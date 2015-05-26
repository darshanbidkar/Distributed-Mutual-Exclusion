import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * @author Akshay Darshan Arnab Aniruddha
 *
 */
public class ControllerProcess implements NetworkInterface {

	// Gson
	private Gson mGson;
	int mHolder;
	private boolean mUsing;
	private boolean mAsked;
	private AbstractProcess mSelf;

	public int mControllerId;
	private NetworkWrapper mNetworkWrapper;

	private Queue<AbstractProcess> mControllerQueue = new LinkedList<AbstractProcess>();

	// HashMaps
	private HashSet<Integer> mProcesses;
	private HashMap<Integer, String> mProcessIpAddresses;
	private HashMap<Integer, Integer> mProcessPorts;
	private HashMap<Integer, String> mConnectorIpAddresses;
	private HashMap<Integer, Integer> mConnectorPorts;

	public ControllerProcess(int pid) {
		addShutdownHook(this);
		// Init server
		this.mNetworkWrapper = NetworkWrapper.getInstance(this);
		// Save info
		this.mControllerId = pid;
		this.mProcessIpAddresses = new HashMap<Integer, String>();
		this.mProcessPorts = new HashMap<Integer, Integer>();
		this.mProcesses = new HashSet<Integer>();
		this.mConnectorIpAddresses = new HashMap<Integer, String>();
		this.mConnectorPorts = new HashMap<Integer, Integer>();

		this.initProcesses();
		this.mGson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
				.create();
		this.mSelf = new AbstractProcess(this.mControllerId);
		this.mUsing = false;
		this.mAsked = false;
	}

	/**
	 * Basic init operations
	 */
	private void initProcesses() {
		this.parseConfigFile();
		System.out.println("");
	}

	private void parseConfigFile() {
		File configFile = new File("controller.config");
		boolean hasReadControllers = false;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(configFile));
			String curr;
			while ((curr = reader.readLine()) != null) {
				String splitCurr[] = curr.split("\t");
				if (splitCurr[0].startsWith("holder"))
					continue;
				int id = Integer.parseInt(splitCurr[0]);
				if (id == this.mControllerId && splitCurr.length == 1) {
					curr = this.readSelf(reader);
					this.mHolder = Integer.parseInt(curr.substring(curr
							.indexOf("=") + 1));
					hasReadControllers = true;
					this.readProcesses(reader);
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

	private void readProcesses(BufferedReader reader) throws IOException {
		String curr = "";
		while (true) {
			curr = reader.readLine();
			if (curr == null)
				return;
			String splitCurr[] = curr.split("\t");
			if (splitCurr.length == 1)
				return;
			int pid = Integer.parseInt(splitCurr[0]);
			this.mProcesses.add(pid);
			this.mProcessIpAddresses.put(pid, splitCurr[1]);
			this.mProcessPorts.put(pid, Integer.parseInt(splitCurr[2]));
		}
	}

	private String readSelf(BufferedReader reader) throws IOException {
		String curr;
		while (!(curr = reader.readLine()).startsWith("holder=")) {
			String splitCurr[] = curr.split("\t");
			int pid = Integer.parseInt(splitCurr[0]);
			this.mConnectorIpAddresses.put(pid, splitCurr[1]);
			this.mConnectorPorts.put(pid, Integer.parseInt(splitCurr[2]));
		}
		return curr;
	}

	// Function that initiates execution of critical section

	private synchronized void sendTokenRequestMessage() {

		JSONObject object = new JSONObject();
		try {

			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_REQUEST_TOKEN);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mConnectorIpAddresses.get(this.mHolder),
					this.mConnectorPorts.get(this.mHolder));
			System.out.println("Container" + this.mControllerId
					+ "sent a token request");

		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	public static void main(String args[]) throws Exception {

		new ControllerProcess(Integer.parseInt(args[0]));

		Scanner scanner = new Scanner(System.in);
		String input = scanner.nextLine();
		if (input.equalsIgnoreCase("end"))
			System.exit(0);
		scanner.close();
	}

	public static void addShutdownHook(final ControllerProcess p) {
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

			case Constants.TYPE_REQUEST_TOKEN:
				processIncomingRequest(object);
				break;

			case Constants.TYPE_TOKEN:
				processIncomingToken(object);
				break;

			case Constants.TYPE_EXIT_CRITICAL_SECTION:
				processIncomingExit(object);
				break;

			case Constants.TYPE_REQUEST_INTERNAL:
				processIncomingRequest(object);
				break;
			}
		} catch (JSONException e) {
			e.printStackTrace();
			System.out.println("Gandla");
		}
	}

	private void processIncomingExit(JSONObject object) {

		this.mUsing = false;
		AbstractProcess topProcess;
		if (!this.mControllerQueue.isEmpty()) {
			topProcess = this.mControllerQueue.remove();
			System.out.println("top Process in controller " + topProcess.pid);
			this.mAsked = false;
			if (!this.mProcesses.contains(topProcess.pid)) {
				sendToken(topProcess);
			}

			else {
				// this.mUsing = true;
				sendPermissionMessage(topProcess);
			}
			System.out.println("Controller id here?:" + mControllerId);
			if (!this.mControllerQueue.isEmpty()) {
				if (this.mHolder != this.mControllerId) {
					sendTokenRequestMessage();
					this.mAsked = true;
				}
			}
		}

	}

	private void processIncomingToken(JSONObject object) {

		AbstractProcess topProcess = this.mControllerQueue.remove();

		this.mHolder = topProcess.pid;

		this.mAsked = false;
		if (this.mProcesses.contains(topProcess.pid)) {
			this.mUsing = true;
			sendPermissionMessage(topProcess);
		} else {
			sendToken(topProcess);
			if (!this.mControllerQueue.isEmpty()) {
				sendTokenRequestMessage();
				this.mAsked = true;
			}
		}

	}

	private void sendPermissionMessage(AbstractProcess other) {
		System.out.println("in Send permission:" + other.pid);
		JSONObject object = new JSONObject();

		try {

			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_PERMISSION);
			System.out.println("Message:" + object.toString() + "ip:"
					+ this.mProcessIpAddresses.get(other.pid) + "port:"
					+ this.mProcessPorts.get(other.pid));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(other.pid),
					this.mProcessPorts.get(other.pid));
			System.out.println("Container" + this.mControllerId
					+ "sent permission to " + other.pid);

		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void sendToken(AbstractProcess other) {

		JSONObject object = new JSONObject();
		try {

			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_TOKEN);
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mConnectorIpAddresses.get(other.pid),
					this.mConnectorPorts.get(other.pid));
			this.mHolder = other.pid;
			System.out.println("Container" + this.mControllerId
					+ "sent the token to " + other.pid);

		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processIncomingRequest(JSONObject object) {

		AbstractProcess incomingProcess = null;
		AbstractProcess topProcess = null;
		try {
			incomingProcess = new AbstractProcess(object.getJSONObject(
					Constants.MESSAGE_INFO).getInt("pid"));

			if (incomingProcess == null)
				System.out.println("NULL INCOMING PROCESS : CHECK : ");

			this.mControllerQueue.add(incomingProcess);

			if (this.mHolder == this.mControllerId && !this.mUsing) {
				topProcess = this.mControllerQueue.remove();
				System.out.println("TopProcess : ");
				System.out.println(topProcess.pid);
				this.mAsked = false;
				// this.mHolder = topProcess.pid;
				if (!this.mProcesses.contains(topProcess.pid)) {
					sendToken(topProcess);
					this.mHolder = topProcess.pid;

					// TODO verify changes here
				} else {
					sendPermissionMessage(topProcess);
					this.mUsing = true;
				}
			} else if (this.mHolder != this.mControllerId && !this.mAsked) {
				sendTokenRequestMessage();
				this.mAsked = true;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	class AbstractProcess {

		@Expose
		private int pid;

		public int getPID() {
			return this.pid;
		}

		public AbstractProcess(int pid) {
			this.pid = pid;
		}

		@Override
		public boolean equals(Object obj) {
			AbstractProcess other = (AbstractProcess) obj;
			return other.getPID() == this.pid;
		}
	}

}
