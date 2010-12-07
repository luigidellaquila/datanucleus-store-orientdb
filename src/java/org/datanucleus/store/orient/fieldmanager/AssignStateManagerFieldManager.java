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
package org.datanucleus.store.orient.fieldmanager;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.api.ApiAdapter;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProviderFactory;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.orient.OrientStoreManager;
import org.datanucleus.store.orient.exceptions.ObjectNotActiveException;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;

/**
 * Field manager that starts from the source object and for all fields will assign StateManagers to all
 * related PersistenceCapable objects found (unless already managed), assuming they are in PersistenceClean state.
 * 
 * @version $Revision: 1.1 $
 **/
public class AssignStateManagerFieldManager extends AbstractFieldManager
{
    /** Orient database. */
    private final ODatabaseObject cont;

    /** StateManager for the owning object whose fields are being fetched. */
    private final ObjectProvider sm;

    /**
     * Constructor.
     * @param cont ObjectContainer
     * @param sm The state manager for the object.
     **/
    public AssignStateManagerFieldManager(ODatabaseObject cont, ObjectProvider sm)
    {
        this.cont = cont;
        this.sm = sm;
    }

    /**
     * Utility method to process the passed persistable object.
     * @param fieldNumber Absolute field number
     * @param pc The persistable object
     */
    protected void processPersistable(int fieldNumber, Object pc)
    {
        OrientStoreManager mgr = (OrientStoreManager)sm.getExecutionContext().getStoreManager();

        // Find the id for this object stored in this field
        Object id = null;
        try
        {
            id = mgr.getObjectIdForObject(sm.getExecutionContext(), pc);
        }
        catch (ObjectNotActiveException onae)
        {
            // Object stored in this field is not active, so mark it as not loaded
            sm.unloadField(sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getName());
        }

        if (id == null)
        {
            // Object is either not activated or not persistent so ignore
            return;
        }


        ExecutionContext ec = sm.getExecutionContext();
//        if (cont.ext().isActive(pc))//TODO
//        {
            // Add StateManager and mark as deactivated for now - why ?
            ObjectProvider newStateManager = ObjectProviderFactory.newForHollowPreConstructed(ec, id, pc);
            mgr.notifyObjectIsOutdated(newStateManager);
//        }
    }

    /**
     * Method to store an object field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeObjectField(int fieldNumber, Object value)
    {
        if (value != null)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ApiAdapter api = ec.getApiAdapter();
            AbstractMemberMetaData fmd = sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            if (api.isPersistable(value))
            {
                // PC field
                ObjectProvider valueSM = ec.findObjectProvider(value);
                if (valueSM == null)
                {
                    // Field is not yet managed
                    processPersistable(fieldNumber, value);
                }
            }
            else if (value instanceof Collection)
            {
                // Collection that may contain PCs
                if (fmd.hasCollection() && fmd.getCollection().elementIsPersistent())
                {
                    Collection coll = (Collection)value;
                    Iterator iter = coll.iterator();
                    while (iter.hasNext())
                    {
                        Object element = iter.next();
                        if (api.isPersistable(element))
                        {
                            ObjectProvider elementSM = ec.findObjectProvider(element);
                            if (elementSM == null)
                            {
                                // Collection Element is not yet managed
                                processPersistable(fieldNumber, element);
                            }
                        }
                    }
                }
            }
            else if (value instanceof Map)
            {
                // Map that may contain PCs in key or value
                if (fmd.hasMap())
                {
                    if (fmd.getMap().keyIsPersistent())
                    {
                        Map map = (Map)value;
                        Set keys = map.keySet();
                        Iterator iter = keys.iterator();
                        while (iter.hasNext())
                        {
                            Object mapKey = iter.next();
                            if (api.isPersistable(mapKey))
                            {
                                ObjectProvider keySM = ec.findObjectProvider(mapKey);
                                if (keySM == null)
                                {
                                    // Map Key is not yet managed
                                    processPersistable(fieldNumber, mapKey);
                                }
                            }
                        }
                    }
                    if (fmd.getMap().valueIsPersistent())
                    {
                        Map map = (Map)value;
                        Collection values = map.values();
                        Iterator iter = values.iterator();
                        while (iter.hasNext())
                        {
                            Object mapValue = iter.next();
                            if (api.isPersistable(mapValue))
                            {
                                ObjectProvider valueSM = ec.findObjectProvider(mapValue);
                                if (valueSM == null)
                                {
                                    // Map Value is not yet managed
                                    processPersistable(fieldNumber, mapValue);
                                }
                            }
                        }
                    }
                }
            }
            else if (value instanceof Object[])
            {
                // Array that may contain PCs
                if (fmd.hasArray() && fmd.getArray().elementIsPersistent())
                {
                    for (int i=0;i<Array.getLength(value);i++)
                    {
                        Object element = Array.get(value, i);
                        if (api.isPersistable(element))
                        {
                            ObjectProvider elementSM = ec.findObjectProvider(element);
                            if (elementSM == null)
                            {
                                // Array element is not yet managed
                                processPersistable(fieldNumber, element);
                            }
                        }
                    }
                }
            }
            else
            {
                // Primitive, or primitive array, or some unsupported container type
            }
        }
    }

    /**
     * Method to store a boolean field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        // Do nothing
    }

    /**
     * Method to store a byte field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeByteField(int fieldNumber, byte value)
    {
        // Do nothing
    }

    /**
     * Method to store a char field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeCharField(int fieldNumber, char value)
    {
        // Do nothing
    }

    /**
     * Method to store a double field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeDoubleField(int fieldNumber, double value)
    {
        // Do nothing
    }

    /**
     * Method to store a float field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeFloatField(int fieldNumber, float value)
    {
        // Do nothing
    }

    /**
     * Method to store an int field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeIntField(int fieldNumber, int value)
    {
        // Do nothing
    }

    /**
     * Method to store a long field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeLongField(int fieldNumber, long value)
    {
        // Do nothing
    }

    /**
     * Method to store a short field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeShortField(int fieldNumber, short value)
    {
        // Do nothing
    }

    /**
     * Method to store a string field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeStringField(int fieldNumber, String value)
    {
        // Do nothing
    }
}
