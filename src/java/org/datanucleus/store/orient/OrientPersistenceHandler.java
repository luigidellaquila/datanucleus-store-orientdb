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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jdo.spi.PersistenceCapable;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.identity.OID;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.StateManagerFactory;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.fieldmanager.PersistFieldManager;
import org.datanucleus.store.orient.fieldmanager.ActivationFieldManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Persistence handler for persisting to Orient datastores.
 */
public class OrientPersistenceHandler extends AbstractPersistenceHandler
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance("org.datanucleus.store.orient.Localisation",
        OrientStoreManager.class.getClassLoader());

    /** Manager for the store. */
    protected final OrientStoreManager storeMgr;

    /**
     * Thread-specific state information (instances of {@link OperationInfo}) for inserting. Allows us to detect the
     * primary object to be inserted, so we can call NeoDatis with that and not for any others.
     */
    private ThreadLocal insertInfoThreadLocal = new ThreadLocal()
    {
        protected Object initialValue()
        {
            return new OperationInfo();
        }
    };

    private static class OperationInfo
    {
        /** List of StateManagers to perform the operation on. */
        List smList = null;
    }

    /**
     * Constructor.
     * @param storeMgr Manager for the datastore
     */
    public OrientPersistenceHandler(StoreManager storeMgr)
    {
        this.storeMgr = (OrientStoreManager) storeMgr;
    }

    /**
     * Method to close the handler and release any resources.
     */
    public void close()
    {
    }

    /**
     * Inserts a persistent object into the database.
     * @param sm The state manager of the object to be inserted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void insertObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        // Get the InsertInfo for this thread so we know if this is the primary object or a reachable
        OperationInfo insertInfo = (OperationInfo) insertInfoThreadLocal.get();
        boolean primaryObject = false;
        if (insertInfo.smList == null)
        {
            // Primary object
            primaryObject = true;
            insertInfo.smList = new ArrayList();
        }
        insertInfo.smList.add(sm);

        String className = sm.getObject().getClass().getName();
        if (!storeMgr.managesClass(className))
        {
            // Class is not yet registered here so register it
            storeMgr.addClass(className, sm.getExecutionContext().getClassLoaderResolver());
            
        }

        sm.provideFields(sm.getClassMetaData().getAllMemberPositions(), new PersistFieldManager(sm, false));

        ManagedConnection mconn = storeMgr.getConnection(sm.getExecutionContext());
        ODatabaseObjectTx connection = (ODatabaseObjectTx) mconn.getConnection();
        try
        {
            connection.save(sm.getObject());
            if (storeMgr.getRuntimeManager() != null)
            {
                storeMgr.getRuntimeManager().incrementInsertCount();
            }

            ObjectProvider objSM = sm.getExecutionContext().findObjectProvider(sm.getObject());
            if (objSM != null)
            {
                AbstractClassMetaData cmd = objSM.getClassMetaData();
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {

                    ORID identity = connection.getRecordByUserObject(sm.getObject(), false).getIdentity();
                    String clusterName = sm.getObject().getClass().getName();
                    long oid = identity.getClusterPosition();
                    if (oid > -1)
                    {
                        objSM.setPostStoreNewObjectId(OIDFactory.getInstance(storeMgr.getOMFContext(), clusterName, oid));
                    }
                    else
                    {
                        NucleusLogger.DATASTORE.error(LOCALISER.msg("Orient.Insert.ObjectPersistFailed", sm.toPrintableID()));
                    }
                }

                VersionMetaData vermd = cmd.getVersionMetaData();
                if (vermd != null && vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    // versioned object so update its version
                    long version = connection.getRecordByUserObject(sm.getObject(), false).getVersion();
                    objSM.setTransactionalVersion(Long.valueOf(version));
                    NucleusLogger.DATASTORE.debug(LOCALISER.msg("Orient.Insert.ObjectPersistedWithVersion", sm.toPrintableID(),
                        objSM.getInternalObjectId(), "" + version));
                }
                else
                {
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER.msg("Orient.Insert.ObjectPersisted", sm.toPrintableID(),
                            objSM.getInternalObjectId()));
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }

        if (primaryObject)
        {
            Iterator iter = insertInfo.smList.iterator();
            while (iter.hasNext())
            {
                ObjectProvider objSM = (ObjectProvider) iter.next();
                objSM.replaceAllLoadedSCOFieldsWithWrappers();
            }

            // Clean out the OperationInfo for inserts on this thread
            insertInfo.smList.clear();
            insertInfo.smList = null;
            insertInfoThreadLocal.remove();
        }
    }

    /**
     * Updates a persistent object in the database.
     * @param sm The state manager of the object to be updated.
     * @param fieldNumbers The numbers of the fields to be updated.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails
     */
    public void updateObject(ObjectProvider sm, int fieldNumbers[])
    {
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        sm.provideFields(fieldNumbers, new PersistFieldManager(sm, false));

        sm.replaceAllLoadedSCOFieldsWithValues();

        ManagedConnection mconn = storeMgr.getConnection(sm.getExecutionContext());
        ODatabaseObjectTx connection = (ODatabaseObjectTx) mconn.getConnection();
        try
        {
            VersionMetaData vermd = sm.getClassMetaData().getVersionMetaData();
            if (sm.getExecutionContext().getTransaction().getOptimistic() && vermd != null)
            {
                // Optimistic transaction so perform version check before any update
                long datastoreVersion = connection.getRecordByUserObject(sm.getObject(), false).getVersion();
                if (datastoreVersion > 0)
                {
                    storeMgr.performVersionCheck(sm, Long.valueOf(datastoreVersion), vermd);
                }
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                AbstractClassMetaData cmd = sm.getClassMetaData();
                StringBuffer fieldStr = new StringBuffer();
                for (int i = 0; i < fieldNumbers.length; i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("Orient.Update.Start", sm.toPrintableID(), sm.getInternalObjectId(),
                    fieldStr.toString()));
            }

            PersistenceCapable pc = (PersistenceCapable) sm.getObject();
            int[] dirtyFieldNumbers = sm.getDirtyFieldNumbers();
            if (dirtyFieldNumbers != null && dirtyFieldNumbers.length > 0)
            {
                Object oid = pc.jdoGetObjectId();
                pc = (PersistenceCapable) findObject(sm.getExecutionContext(), oid);
                ObjectProvider newSm = sm.getExecutionContext().findObjectProvider(pc);
                for (int fieldNum : dirtyFieldNumbers)
                {
                    Object fieldVal = sm.provideField(fieldNum);
                    newSm.replaceField(fieldNum, fieldVal);
                }
                // sm.replaceManagedPC(pc); //TODO should it be done...?

            }
            else
            {
                // Just needs updating
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(LOCALISER.msg("Orient.Object.Refreshing", StringUtils.toJVMIDString(pc)));
                }
                Object oid = pc.jdoGetObjectId();
                pc = (PersistenceCapable) findObject(sm.getExecutionContext(), oid);
            }

            // Do the update in Orient
            connection.save(pc);
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("Orient.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (storeMgr.getRuntimeManager() != null)
            {
                storeMgr.getRuntimeManager().incrementUpdateCount();
            }

            if (vermd != null && vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
            {
                // versioned object so update its version now that we've persisted the changes
                long version = connection.getRecordByUserObject(sm.getObject(), false).getVersion();
                sm.setTransactionalVersion(Long.valueOf(version));
            }

            // Wrap any unwrapped SCO fields so any subsequent changes are picked up
            // sm.replaceAllLoadedSCOFieldsWithWrappers();
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Deletes a persistent object from the database.
     * @param sm The state manager of the object to be deleted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails on an optimistic transaction for this object
     */
    public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        ManagedConnection mconn = storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            ODatabaseObjectTx connection = (ODatabaseObjectTx) mconn.getConnection();
            VersionMetaData vermd = sm.getClassMetaData().getVersionMetaData();
            if (sm.getExecutionContext().getTransaction().getOptimistic() && vermd != null)
            {
                // Optimistic transaction so perform version check before any delete
                long datastoreVersion = connection.getRecordByUserObject(sm.getObject(), false).getVersion();
                if (datastoreVersion > 0)
                {
                    storeMgr.performVersionCheck(sm, Long.valueOf(datastoreVersion), vermd);
                }
            }

            // Load any unloaded fields so that DeleteFieldManager has all field values to work with
            sm.loadUnloadedFields();

            // Delete all reachable PC objects (due to dependent-field). Updates the StateManagers to be in deleted
            // state
            sm.provideFields(sm.getClassMetaData().getAllMemberPositions(), new DeleteFieldManager(sm));

            // This delete is for the root object so just persist to Orient and it will delete all dependent
            // objects for us
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("Orient.Delete.Start", sm.toPrintableID(), sm.getInternalObjectId()));
            }
            connection.delete(sm.getObject());
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("Orient.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (storeMgr.getRuntimeManager() != null)
            {
                storeMgr.getRuntimeManager().incrementDeleteCount();
            }

        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Fetches fields of a persistent object from the database.
     * @param sm The state manager of the object to be fetched.
     * @param fieldNumbers The numbers of the fields to be fetched.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void fetchObject(ObjectProvider sm, int fieldNumbers[])
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            StringBuffer str = new StringBuffer("Fetching object \"");
            str.append(sm.toPrintableID()).append("\" (id=");
            str.append(sm.getExecutionContext().getApiAdapter().getObjectId(sm)).append(")").append(" fields [");
            for (int i = 0; i < fieldNumbers.length; i++)
            {
                if (i > 0)
                {
                    str.append(",");
                }
                str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
            }
            str.append("]");
            NucleusLogger.PERSISTENCE.debug(str);
        }

        ManagedConnection mconn = storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            final ODatabaseObjectTx connection = (ODatabaseObjectTx) mconn.getConnection();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER.msg("Orient.Fetch.Start", sm.toPrintableID(), sm.getInternalObjectId()));
            }

            // Process all requested fields so they are managed (loaded) and active now
            sm.replaceFields(fieldNumbers, new ActivationFieldManager(connection, sm));

            VersionMetaData vermd = cmd.getVersionMetaData();
            if (vermd != null && vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
            {
                // Object needs versioning so store its current datastore version in the StateManager
                long version = connection.getRecordByUserObject(sm.getObject(), false).getVersion();
                sm.setTransactionalVersion(Long.valueOf(version));
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER.msg("Orient.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (storeMgr.getRuntimeManager() != null)
            {
                storeMgr.getRuntimeManager().incrementFetchCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Accessor for an (at least) hollow PersistenceCapable object matching the given id.
     * @param ec the ExecutionContext which will manage the object
     * @param id the id of the object in question.
     * @return a persistable object with a valid state (for example: hollow) or null, indicating that the implementation
     * leaves the instantiation work to us.
     */
    public Object findObject(ExecutionContext ec, Object id)
    {
        Object pc = null;

        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            ODatabaseObjectTx cont = (ODatabaseObjectTx) mconn.getConnection();

            if (id instanceof String)
            {
                String[] splitted = ((String) id).split("\\[OID\\]");// TODO CONSTANT!!!
                long recordId = Long.parseLong(splitted[0]);
                String className = splitted[1];
                String[] classSplitted = className.split("\\.");
                String clusterName = classSplitted[classSplitted.length - 1];
                int clusterId = cont.getClusterIdByName(clusterName.toLowerCase());
                ORecordId orid = new ORecordId(clusterId, recordId);

                pc = cont.load(orid);
                if (pc == null)
                {
                    return null;
                }
                if (ec.findObjectProvider(pc) == null)
                {
                    StateManagerFactory.newStateManagerForHollowPreConstructed(ec, id, pc);
                }
            }
            else if (id instanceof OID)
            {
                OID oid = (OID) id;
                long recordId = (Long) oid.getKeyValue();
                String className = oid.getPcClass();
                String[] classSplitted = className.split("\\.");
                String clusterName = classSplitted[classSplitted.length - 1];
                int clusterId = cont.getClusterIdByName(clusterName);
                ORecordId orid = new ORecordId(clusterId, recordId);

                pc = cont.load(orid);
                if (pc == null)
                {
                    return null;
                }
                if (ec.findObjectProvider(pc) == null)
                {
                    StateManagerFactory.newStateManagerForHollowPreConstructed(ec, id, pc);
                }
            }
            else
            {
                System.out.println("TODO!!");
                throw new RuntimeException("TODO!!!!");// TODO
            }
        }
        finally
        {
            mconn.release();
        }
        return pc;
    }

    /**
     * Locates this object in the datastore.
     * @param sm The StateManager for the object to be found
     * @throws NucleusObjectNotFoundException if the object doesnt exist
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void locateObject(ObjectProvider sm)
    {
        ExecutionContext ec = sm.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractClassMetaData cmd = sm.getClassMetaData();

        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            ODatabaseObjectTx connection = (ODatabaseObjectTx) mconn.getConnection();
            // TODO
            // if(is detached)
            // then reattach
            // end

            if (connection.getRecordByUserObject(sm.getObject(), true) == null)
            {
                throw new NucleusObjectNotFoundException(LOCALISER.msg("Orient.Object.NotFound", sm.toPrintableID(),
                    sm.getInternalObjectId()));
            }

        }
        finally
        {
            mconn.release();
        }
    }
}
