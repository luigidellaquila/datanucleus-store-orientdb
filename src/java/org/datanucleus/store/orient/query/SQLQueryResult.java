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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.QueryResultMetaData;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.orient.OrientStoreManager;
import org.datanucleus.store.orient.OrientUtils;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.SoftValueMap;
import org.datanucleus.util.StringUtils;
import org.datanucleus.util.WeakValueMap;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;

/**
 * Result from an SQL query with Orient. Takes the sql4o result and converts it into the result format expected by JDO/JPA
 * SQL definitions. Provides caching of results in this object, controlled by property
 * "datanucleus.query.resultCacheType".
 */
public class SQLQueryResult extends AbstractQueryResult
{
    /** Whether to load any unread results at commit (when connection is closed). */
    private boolean loadResultsAtCommit = true; // Default to load

    /** Map of ResultSet object values, keyed by the list index ("0", "1", etc). */
    private Map resultsObjsByIndex = null;

    /** Size of results, if known. -1 otherwise. */
    protected int size = -1;

    /** ODatabase Container, to obtain any more information from where required. */
    ODatabaseObjectTx objectContainer;


    List results;

    QueryResultMetaData resultMetaData;

    Class resultClass;

    Class candidateClass;

    /**
     * Constructor.
     * @param query Query being executed
     * @param cont Object Container in use
     * @param results The results from sql4o
     */
    public SQLQueryResult(Query query, ODatabaseObjectTx cont, List results, QueryResultMetaData resultMetaData)
    {
        super(query);
        this.objectContainer = cont;
        this.results = results;
        this.resultMetaData = resultMetaData;
        this.resultClass = query.getResultClass();
        this.candidateClass = query.getCandidateClass();

        String ext = (String) query.getExtension("datanucleus.query.loadResultsAtCommit");
        if (ext != null)
        {
            loadResultsAtCommit = new Boolean(ext).booleanValue();
        }

        ext = (String) query.getExtension("datanucleus.query.resultCacheType");
        if (ext != null)
        {
            if (ext.equalsIgnoreCase("soft"))
            {
                resultsObjsByIndex = new SoftValueMap();
            }
            else if (ext.equalsIgnoreCase("weak"))
            {
                resultsObjsByIndex = new WeakValueMap();
            }
            else if (ext.equalsIgnoreCase("hard"))
            {
                resultsObjsByIndex = new HashMap();
            }
            else if (ext.equalsIgnoreCase("none"))
            {
                resultsObjsByIndex = null;
            }
            else
            {
                resultsObjsByIndex = new WeakValueMap();
            }
        }
        else
        {
            resultsObjsByIndex = new WeakValueMap();
        }

        size = results.size();

        if (resultsObjsByIndex != null)
        {
            // Caching results so load up any result objects needed right now
            int fetchSize = query.getFetchPlan().getFetchSize();
            if (!query.getObjectManager().getTransaction().isActive() || fetchSize == FetchPlan.FETCH_SIZE_GREEDY)
            {
                // No transaction or in "greedy" mode so load all results now
                for (int i = 0; i < size; i++)
                {
                    getObjectForIndex(i);
                }
            }
            else if (fetchSize > 0)
            {
                // Load up the first "fetchSize" objects now
                for (int i = 0; i < fetchSize; i++)
                {
                    getObjectForIndex(i);
                }
            }
        }
    }

    /**
     * Close the results and free off any resources held.
     */
    public void close()
    {
        if (resultsObjsByIndex != null)
        {
            resultsObjsByIndex.clear();
        }

        super.close();
    }

    /**
     * Internal method to close the ResultSet.
     */
    protected void closeResults()
    {
        if (results != null)
        {
            // Nothing more to do than null out
            results = null;
        }
    }

    /**
     * Inform the query result that the connection is being closed so perform any operations now, or rest in peace.
     */
    protected void closingConnection()
    {
        // Make sure all rows are loaded.
        if (loadResultsAtCommit && isOpen())
        {
            // Query connection closing message
            NucleusLogger.QUERY.info(LOCALISER.msg("052606", query.toString()));

            for (int i = 0; i < size(); i++)
            {
                getObjectForIndex(i);
            }
        }
    }

    /**
     * Equality operator for QueryResults. Overrides the AbstractList implementation since that uses size() and
     * iterator() and that would cause problems when closed.
     * @param o The object to compare against
     * @return Whether they are equal
     */
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof SQLQueryResult))
        {
            return false;
        }

        SQLQueryResult other = (SQLQueryResult) o;
        if (query != null)
        {
            return other.query == query;
        }
        return StringUtils.toJVMIDString(other).equals(StringUtils.toJVMIDString(this));
    }

    /**
     * Accessor for an iterator for the results.
     * @return The iterator
     */
    public Iterator iterator()
    {
        assertIsOpen();
        return new QueryResultIterator();
    }

    /**
     * Accessor for an iterator for the results.
     * @return The iterator
     */
    public ListIterator listIterator()
    {
        assertIsOpen();
        return new QueryResultIterator();
    }

    /**
     * An Iterator results of a pm.query.execute().iterator()
     */
    private class QueryResultIterator implements ListIterator
    {
        private int iterRowNum = 0; // The index of the next object

        /**
         * Constructor
         */
        public QueryResultIterator()
        {
        }

        public void add(Object obj)
        {
            throw new UnsupportedOperationException(LOCALISER.msg("052603"));
        }

        public boolean hasNext()
        {
            synchronized (SQLQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasNext() on closed Query will return false
                    return false;
                }

                // When we aren't at size()-1 we have at least one more element
                return (iterRowNum <= (size() - 1));
            }
        }

        public boolean hasPrevious()
        {
            synchronized (SQLQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasPrevious() on closed Query will return false
                    return false;
                }

                // A List has indices starting at 0 so when we have > 0 we have a previous
                return (iterRowNum > 0);
            }
        }

        public Object next()
        {
            synchronized (SQLQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling next() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(LOCALISER.msg("052600"));
                }

                if (!hasNext())
                {
                    throw new NoSuchElementException("No next element");
                }
                Object obj = getObjectForIndex(iterRowNum);
                iterRowNum++;

                return obj;
            }
        }

        public int nextIndex()
        {
            if (hasNext())
            {
                return iterRowNum;
            }
            return size();
        }

        public Object previous()
        {
            synchronized (SQLQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling previous() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(LOCALISER.msg("052600"));
                }

                if (!hasPrevious())
                {
                    throw new NoSuchElementException("No previous element");
                }

                iterRowNum--;
                return getObjectForIndex(iterRowNum);
            }
        }

        public int previousIndex()
        {
            if (iterRowNum == 0)
            {
                return -1;
            }
            return iterRowNum - 1;
        }

        public void remove()
        {
            throw new UnsupportedOperationException(LOCALISER.msg("052603"));
        }

        public void set(Object obj)
        {
            throw new UnsupportedOperationException(LOCALISER.msg("052603"));
        }
    }

    /**
     * Accessor for the result object at an index. If the object has already been processed will return that object,
     * otherwise will retrieve the object using the factory.
     * @param index The list index position
     * @return The result object
     */
    protected Object getObjectForIndex(int index)
    {
        Object obj = null;
        if (resultsObjsByIndex != null)
        {
            // Caching objects, so check the cache for this index
            obj = resultsObjsByIndex.get("" + index);
            if (obj != null)
            {
                // Already retrieved so return it
                return obj;
            }
        }

        if (resultMetaData != null)
        {
            // Each row of the ResultSet is defined by MetaData
            obj = getRowForResultMetaData(index);
        }
        else if (resultClass != null)
        {
            // Each row of the ResultSet is an instance of resultClass
            // TODO
        }
        else if (candidateClass == null)
        {
            // Each row of the ResultSet is an Object or Object[]
            // TODO
        }
        else
        {
            // Each row of the ResultSet is an instance of the candidate class
            obj = getRowForCandidateClass(index);
        }

        if (resultsObjsByIndex != null)
        {
            // Put it in our cache, keyed by the list index
            resultsObjsByIndex.put("" + index, obj);
        }

        return obj;
    }

    /**
     * Accessor for the row object(s) when there is metadata defining the result. Each row of results should follow the
     * result metadata (JPA).
     * @param index Row index
     * @return The row in the required form
     */
    protected Object getRowForResultMetaData(int index)
    {
        // TODO Implement this
        return null;
    }

    /**
     * Accessor for the row object(s) when there is a candidate class. Each row of results should be of candidate type.
     * @param index Row index
     * @return The row in the required form
     */
    protected Object getRowForCandidateClass(int index)
    {
        // Current sql4o doesnt support joins and so when we have a candidate class each row
        // is an object of the candidate type.
        // TODO Cater for case where user only selects identity field(s)
        Object obj = results.get(index);
        // TODO
        return obj;
    }

    public synchronized Object get(int index)
    {
        assertIsOpen();
        return getObjectForIndex(index);
    }

    public int size()
    {
        assertIsOpen();
        return results.size();
    }

    public Object[] toArray()
    {
        assertIsOpen();
        // TODO Auto-generated method stub
        return null;
    }

    public Object[] toArray(Object[] arg0)
    {
        assertIsOpen();
        // TODO Auto-generated method stub
        return null;
    }
}
