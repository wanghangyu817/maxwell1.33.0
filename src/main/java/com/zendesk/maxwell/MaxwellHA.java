package com.zendesk.maxwell;

import com.zendesk.maxwell.util.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxwellHA {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHA.class);

	private final Maxwell maxwell;
	private final String zookeeperServer,clientId;
	private final int sessionTimeoutMs, connectionTimeoutMs, maxRetries, baseSleepTimeMs;
	private boolean hasRun = false;

	public MaxwellHA(Maxwell maxwell, String zookeeperServer, int sessionTimeoutMs, int connectionTimeoutMs, int maxRetries, int baseSleepTimeMs, String clientId) {
		this.maxwell = maxwell;
		this.zookeeperServer = zookeeperServer;
		this.sessionTimeoutMs = sessionTimeoutMs;
		this.connectionTimeoutMs = connectionTimeoutMs;
		this.maxRetries = maxRetries;
		this.baseSleepTimeMs = baseSleepTimeMs;
		this.clientId = clientId;
	}

	private void run() {
		try {
			if (hasRun)
				maxwell.restart();
			else
				maxwell.start();

			hasRun = true;
		} catch ( Exception e ) {
			LOGGER.error("Maxwell terminating due to exception:", e);
			System.exit(1);
		}
	}

	public void startHA() throws Exception {

		CuratorUtils cu = new CuratorUtils();
		cu.setZookeeperServer(zookeeperServer);
		cu.setSessionTimeoutMs(sessionTimeoutMs);
		cu.setConnectionTimeoutMs(connectionTimeoutMs);
		cu.setMaxRetries(maxRetries);
		cu.setBaseSleepTimeMs(baseSleepTimeMs);
		cu.setClientId(clientId);
		cu.init();
		CuratorFramework client = cu.getClient();
		LeaderLatch leader = new LeaderLatch(client, cu.getElectPath());
		leader.start();
		leader.addListener(new LeaderLatchListener() {
			@Override
			public void isLeader() {
				cu.register();
				run();
				LOGGER.info("starting maxwell");
			}

			@Override
			public void notLeader() {

			}
		});

		Thread.sleep(Long.MAX_VALUE);
	}
}
