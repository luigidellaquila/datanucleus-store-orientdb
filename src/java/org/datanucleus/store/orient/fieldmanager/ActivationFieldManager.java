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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.api.ApiAdapter;
import org.datanucleus.state.StateManagerFactory;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;
import org.datanucleus.store.orient.OrientStoreManager;
import org.datanucleus.store.orient.exceptions.ObjectNotActiveException;
import org.datanucleus.store.types.sco.SCO;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;

/**
 */
public class ActivationFieldManager extends AbstractFieldManager
{
    
    private ODatabaseObject cont;

    /** StateManager of the owning object whose fields are being fetched. */
    private ObjectProvider sm;

    /**
     * Constructor
     */
    public ActivationFieldManager(ODatabaseObject cont, ObjectProvider sm)
    {
        this.cont = cont;
        this.sm = sm;
    }

    /**
     * Utility method to process the passed persistable object.
     * @param fieldNumber Number of the field with this value
     * @param pc The PC object
     */
    protected void processPersistable(int fieldNumber, Object pc)
    {
        ExecutionContext ec = sm.getExecutionContext();
        OrientStoreManager mgr = (OrientStoreManager)ec.getStoreManager();


        ObjectProvider pcSM = ec.findObjectProvider(pc);
        if (pcSM == null)
        {
            // Object has no StateManager so create one
            Object id = null;
            try
            {
                id = mgr.getObjectIdForObject(ec, pc);
            }
            catch (ObjectNotActiveException onae)
            {
                // Object stored in this field is not active, so mark it as not loaded
                sm.unloadField(sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getName());
            }

            if (id == null)
            {
                // Object not persistent yet ?
                return;
            }

            // Add StateManager
            // TODO Localise this message
            NucleusLogger.DATASTORE.debug("Field " + sm.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getFullFieldName() + 
                " with value " + StringUtils.toJVMIDString(pc) + " and id=" + id + " has no StateManager so attaching one");
            StateManagerFactory.newStateManagerForHollowPreConstructed(ec, id, pc);
        }
    }

    public Object fetchObjectField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        Object value = sfv.fetchObjectField(fieldNumber);
        ApiAdapter api = sm.getExecutionContext().getApiAdapter();

        if (value != null)
        {
            if (api.isPersistable(value))
            {
                // Process PC fields
                processPersistable(fieldNumber, value);
            }
            else if (value instanceof Collection)
            {
                // Process all elements of the Collection that are PC
//                cont.ext().activate(value, 2); //  TODO
                if (!(value instanceof SCO))
                {
                    // Replace with SCO
                    value = sm.wrapSCOField(fieldNumber, value, false, false, true);
                }
                Collection coll = (Collection)value;
                Iterator iter = coll.iterator();
                while (iter.hasNext())
                {
                    Object element = iter.next();
                    if (api.isPersistable(element))
                    {
                        processPersistable(fieldNumber, element);
                    }
                }
            }
            else if (value instanceof Map)
            {
                // Process all keys, values of the Map that are PC
                //cont.ext().activate(value, 2); // TODO 
                if (!(value instanceof SCO))
                {
                    // Replace with SCO
                    value = sm.wrapSCOField(fieldNumber, value, false, false, true);
                }
                Map map = (Map)value;

                // Process any keys that are PersistenceCapable
                Set keys = map.keySet();
                Iterator iter = keys.iterator();
                while (iter.hasNext())
                {
                    Object mapKey = iter.next();
                    if (api.isPersistable(mapKey))
                    {
                        processPersistable(fieldNumber, mapKey);
                    }
                }

                // Process any values that are PersistenceCapable
                Collection values = map.values();
                iter = values.iterator();
                while (iter.hasNext())
                {
                    Object mapValue = iter.next();
                    if (api.isPersistable(mapValue))
                    {
                        processPersistable(fieldNumber, mapValue);
                    }
                }
            }
            else if (value instanceof Object[])
            {
                //cont.ext().activate(value, 2); // TODO 
                Object[] array = (Object[]) value;
                for (int i=0;i<array.length;i++)
                {
                    Object element = array[i];
                    if (api.isPersistable(element))
                    {
                        processPersistable(fieldNumber, element);
                    }
                }
            }
            else if (value instanceof SCO)
            {
                // Other SCO field
            }
            else
            {
                // Primitive, or primitive array, or some unsupported container type
            }
        }
        return value;
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchBooleanField(fieldNumber);
    }

    public byte fetchByteField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchByteField(fieldNumber);
    }

    public char fetchCharField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchCharField(fieldNumber);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchDoubleField(fieldNumber);
    }

    public float fetchFloatField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchFloatField(fieldNumber);
    }

    public int fetchIntField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchIntField(fieldNumber);
    }

    public long fetchLongField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchLongField(fieldNumber);
    }

    public short fetchShortField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchShortField(fieldNumber);
    }

    public String fetchStringField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        sm.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchStringField(fieldNumber);
    }
}
