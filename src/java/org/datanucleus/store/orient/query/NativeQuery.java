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
package org.datanucleus.store.orient.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.orient.OrientStoreManager;
import org.datanucleus.store.orient.OrientUtils;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.util.Localiser;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.query.OQuery;

/**
 */
public class NativeQuery extends AbstractJavaQuery
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_ORIENT = Localiser.getInstance(
        "org.datanucleus.store.orient.Localisation", OrientStoreManager.class.getClassLoader());

    /** The Predicate for the native query. */
    protected OQuery predicate = null;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param ec execution context
     */
    public NativeQuery(ExecutionContext ec)
    {
        this(ec, null);
    }

    /**
     * Constructor for a query using OrientDB "native" query language.
     * The second parameter must implement "com.orientechnologies.orient.core.query.OQuery".
     * @param ec execution context
     * @param predicate The native query predicate
     * @throws NucleusUserException When the second parameter isnt an implementation of a Predicate
     */
    public NativeQuery(ExecutionContext ec, Object predicate)
    {
        super(ec);

        if (!(predicate instanceof OQuery))
        {
            throw new NucleusUserException(LOCALISER_ORIENT.msg("Orient.Native.NeedsPredicate"));
        }

        this.predicate = (OQuery)predicate;
//        setCandidateClassName(this.predicate);//TODO
    }


    /**
     * Method to return if the query is compiled.
     * @return Whether it is compiled
     */
    protected boolean isCompiled()
    {
        return true;
    }

    /**
     * Method to execute the query.
     * @param parameters Map of parameter values keyed by the name
     * @return The query result
     */
    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = ec.getStoreManager().getConnection(ec);
        ODatabaseObjectTx cont = (ODatabaseObjectTx) mconn.getConnection();
        try
        {

            ArrayList results = new ArrayList();
            AbstractClassMetaData cmd = null;
            Iterator iter = cont.query(predicate).iterator();
            while (iter.hasNext())
            {
                Object obj = iter.next();
                if (ec.getApiAdapter().isPersistable(obj))
                {
                    if (cmd == null)
                    {
                        cmd = ec.getMetaDataManager().getMetaDataForClass(getCandidateClassName(), 
                            ec.getClassLoaderResolver());
                    }
                    OrientUtils.prepareOrientObjectForUse(obj, ec, cont, cmd, (OrientStoreManager)ec.getStoreManager());
                }
            }

            return results;
            
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Convenience method to return whether the query should return a single row.
     * @return Whether a single row should result
     */
    protected boolean shouldReturnSingleRow()
    {
        // We always return the List of objects returned by the query since no other way of knowing if unique
        return false;
    }

    /**
     * Method to return the query as a single string.
     * @return The single string
     */
    public String getSingleStringQuery()
    {
        return "Orient Native Query <" + candidateClassName + ">";
    }

    /**
     * Method to return the names of the extensions supported by this query.
     * To be overridden by subclasses where they support additional extensions.
     * @return The supported extension names
     */
    public Set<String> getSupportedExtensions()
    {
        Set<String> supported = super.getSupportedExtensions();
        supported.add("orient.native.comparator");
        return supported;
    }

    @Override
    protected void compileInternal(Map parameterValues)
    {
        
    }
}
