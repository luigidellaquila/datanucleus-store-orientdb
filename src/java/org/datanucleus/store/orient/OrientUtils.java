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

import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProviderFactory;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.orient.fieldmanager.AssignStateManagerFieldManager;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Utilities for Orient (http://www.orientechnologies.com).
 */
public class OrientUtils
{
    /**
     * Convenience method to take an object returned by Orient (from a query for example), and prepare it for passing to
     * the user. Makes sure there is a StateManager connected, with associated fields marked as loaded.
     * @param obj The object (from Orient)
     * @param ec execution context
     * @param cont ObjectContainer that returned the object
     * @param cmd ClassMetaData for the object
     * @param mgr OrientStoreManager
     * @return The StateManager for this object
     */
    public static ObjectProvider prepareOrientObjectForUse(Object obj, ExecutionContext ec, ODatabaseObjectTx cont,
            AbstractClassMetaData cmd, OrientStoreManager mgr)
    {
        if (!ec.getApiAdapter().isPersistable(obj))
        {
            return null;
        }

        ObjectProvider sm = ec.findObjectProvider(obj);
        if (sm == null)
        {
            // Find the identity
            Object id = null;
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                ORecordId orid = (ORecordId) cont.getRecordByUserObject(obj, false).getIdentity();
                String orientId = orid.toString();
                String[] oidParts = orientId.split(":");
                String jdoOid = "";
                jdoOid += oidParts[1];
                jdoOid += "[OID]";
                jdoOid += obj.getClass().getName();
                
                id = OIDFactory.getInstance(ec.getOMFContext(), jdoOid);
            }
            else
            {
                id = ec.getApiAdapter().getNewApplicationIdentityObjectId(obj, cmd);
            }

            // Object not managed so give it a StateManager before returning it
            sm = ObjectProviderFactory.newForPersistentClean(ec, id, obj);
            sm.provideFields(cmd.getAllMemberPositions(), new AssignStateManagerFieldManager(cont, sm));
        }

        sm.replaceAllLoadedSCOFieldsWithWrappers();

        return sm;
    }
}
