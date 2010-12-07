/**********************************************************************
Copyright (c) 2010 Luigi Dell'Aquila and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
 **********************************************************************/
package org.datanucleus.store.orient;

import java.util.Map;

import javax.transaction.xa.XAResource;

import org.datanucleus.OMFContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;

/**
 * Implementation of a ConnectionFactory for Orient Database. </p>
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory {
	protected static final Localiser	LOCALISER_ORIENT	= Localiser.getInstance("org.datanucleus.store.orient.Localisation",
																													OrientStoreManager.class.getClassLoader());

	private String										url;

	private String										username;

	private String										password;

	/**
	 * Constructor
	 * 
	 * @param omfContext
	 *          The OMF context
	 * @param resourceType
	 *          Type of resource (tx, nontx)
	 */
	public ConnectionFactoryImpl(OMFContext omfContext, String resourceType) {
		super(omfContext, resourceType);

		this.url = omfContext.getStoreManager().getConnectionURL();
		this.username = omfContext.getStoreManager().getConnectionUserName();
		this.password = omfContext.getStoreManager().getConnectionPassword();
		if (!(url.startsWith("remote:") || url.startsWith("local:"))) {
			throw new NucleusException(LOCALISER_ORIENT.msg("Orient.URLInvalid", url));
		}
	}

	/**
	 * Obtain a connection from the Factory. The connection will be enlisted within the {@link org.datanucleus.Transaction} associated
	 * to the <code>poolKey</code> if "enlist" is set to true.
	 * 
	 * @param poolKey
	 *          the pool that is bound the connection during its lifecycle (or null)
	 * @param options
	 *          Any options for then creating the connection
	 * @return the {@link org.datanucleus.store.connection.ManagedConnection}
	 */
	public ManagedConnection createManagedConnection(Object poolKey, @SuppressWarnings("rawtypes") Map options) {
		return new ManagedConnectionImpl(omfContext, options);
	}

	/**
	 * Implementation of a ManagedConnection for Orient.
	 */
	class ManagedConnectionImpl extends AbstractManagedConnection {
		OMFContext	omf;

		/**
		 * Constructor.
		 * 
		 * @param omf
		 * @param poolKey
		 * @param transactionOptions
		 *          Any options
		 */
		ManagedConnectionImpl(OMFContext omf, @SuppressWarnings("rawtypes") Map transactionOptions) {
			this.omf = omf;
		}

		/**
		 * Obtain a XAResource which can be enlisted in a transaction
		 */
		public XAResource getXAResource() {
			return null;
		}

		public ODatabaseObject getOrientConnection() {
			return (ODatabaseObject) conn;
		}

		/**
		 * Create a connection to the resource
		 */
		public Object getConnection() {
			if (conn == null) {
				NucleusLogger.CONNECTION.debug(LOCALISER_ORIENT.msg("Orient.connecting", url, username));// TODO
				conn = new ODatabaseObjectTx(url).open(username, password);
				NucleusLogger.CONNECTION.info(LOCALISER_ORIENT.msg("Orient.connected", url, username));// TODO
				((OrientStoreManager) omf.getStoreManager()).registerObjectContainer((ODatabaseObjectTx) conn);
			}
			return conn;
		}

		/**
		 * Close the connection
		 */
		public void close() {
			for (int i = 0; i < listeners.size(); i++) {
				listeners.get(i).managedConnectionPreClose();
			}

			ODatabaseObject conn = getOrientConnection();
			if (conn != null) {

				String connStr = conn.toString();
				if (commitOnRelease) {
					if (!conn.isClosed()) {
						conn.commit();
						if (NucleusLogger.CONNECTION.isDebugEnabled()) {
							NucleusLogger.CONNECTION.debug(LOCALISER_ORIENT.msg("Orient.commitOnClose", connStr));// TODO
						}
					}
				}

				if (!conn.isClosed()) {
					conn.close();
					if (NucleusLogger.CONNECTION.isDebugEnabled()) {
						NucleusLogger.CONNECTION.debug(LOCALISER_ORIENT.msg("Orient.closingConnection", connStr));// TODO
					}
				} else {
					if (NucleusLogger.CONNECTION.isDebugEnabled()) {
						NucleusLogger.CONNECTION.debug(LOCALISER_ORIENT.msg("Orient.connectionAlreadyClosed", connStr));// TODO
					}
				}

			}

			try {
				for (int i = 0; i < listeners.size(); i++) {
					listeners.get(i).managedConnectionPostClose();
				}
			} finally {
				listeners.clear();
			}
			((OrientStoreManager) omf.getStoreManager()).deregisterObjectContainer((ODatabaseObjectTx) conn);
			this.conn = null;
		}
	}

}
