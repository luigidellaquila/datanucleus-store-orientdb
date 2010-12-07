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

import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.orient.OrientStoreManager;
import org.datanucleus.store.query.AbstractSQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * SQL query implementation for Orient datastores.
 */
public class SQLQuery extends AbstractSQLQuery
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_ORIENT = Localiser.getInstance(
        "org.datanucleus.store.orient.Localisation", OrientStoreManager.class.getClassLoader());

    /** State variable for the compilation state */
    protected transient boolean isCompiled = false;

    /**
     * Constructor for a new query using the existing query.
     * @param ec execution context
     * @param query The existing query
     */
    public SQLQuery(ExecutionContext ec, SQLQuery query)
    {
        super(ec, query);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param ec execution context
     */
    public SQLQuery(ExecutionContext ec)
    {
        super(ec, (String)null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param ec execution context
     * @param sqlText The SQL query string
     */
    public SQLQuery(ExecutionContext ec, String sqlText)
    {
        super(ec, sqlText);

//         Check if sql4o JAR is in CLASSPATH
//        ClassUtils.assertClassForJarExistsInClasspath(ec.getClassLoaderResolver(), 
//            "org.datanucleus.sql4o.Sql4o", "sql4o.jar");

        String firstToken = new StringTokenizer(sqlText, " ").nextToken();
        if (!firstToken.equals("SELECT") && !firstToken.startsWith("select"))
        {
            throw new NucleusUserException(LOCALISER.msg("059002", inputSQL));
        }
    }

    /**
     * Equality operator.
     * Returns true if the other object is of this type and if the input SQL string is the same.
     * @param obj The object to compare against
     * @return Whether they are equal
     */
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof SQLQuery) || !super.equals(obj))
        {
            return false;
        }

        return inputSQL.equals(((SQLQuery)obj).inputSQL);
    }

    /**
     * Utility to remove any previous compilation of this Query.
     **/
    protected void discardCompiled()
    {
        isCompiled = false;
        super.discardCompiled();
    }

    /**
     * Method to return if the query is compiled.
     * @return Whether it is compiled
     */
    protected boolean isCompiled()
    {
        return isCompiled;
    }

    /**
     * Verify the elements of the query and provide a hint to the query to prepare and optimize an execution plan.
     * TODO Drop this method for 2.2 onwards
     */
    public void compileInternal(boolean forExecute, Map parameterValues)
    {
        compileInternal(parameterValues);
    }

    /**
     * Verify the elements of the query and provide a hint to the query to prepare and optimize an execution plan.
     */
    public void compileInternal(Map parameterValues)
    {
        if (isCompiled)
        {
            return;
        }

        compiledSQL = generateQueryStatement();
        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            NucleusLogger.QUERY.debug(LOCALISER.msg("059012", compiledSQL));
        }

        isCompiled = true;
    }

    /**
     * Execute the query and return the result.
     * For a SELECT query this will be the QueryResult. For an UPDATE/DELETE it will be the row count for
     * the update statement.
     * @param parameters the Map containing all of the parameters (positional parameters)
     * @return the result of the query
     */
    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = ec.getStoreManager().getConnection(ec);
        ODatabaseObjectTx cont = (ODatabaseObjectTx) mconn.getConnection();
        try
        {
            List results = cont.query(new OSQLSynchQuery<Object>(compiledSQL));
            return new SQLQueryResult(this, cont, results, resultMetaData);
        }
        catch (Exception sqlpe)
        {
            throw new NucleusDataStoreException(LOCALISER.msg("059025", compiledSQL), sqlpe);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to perform any necessary pre-processing on the users query statement
     * before we execute it. SQL queries are not modified in any way, as per JDO2 spec [14.7].
     * @return The compiled SQL statement
     */
    protected String generateQueryStatement()
    {
        // We're returning the users SQL direct with no substitution of params etc
        String compiledSQL = getInputSQL();

        if (candidateClass != null && getType() == Query.SELECT)
        {
            // Perform any sanity checking of input for SELECT queries
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);

            if (getResultClass() == null)
            {
                // Check the presence of the required columns (id, version, discriminator) in the candidate class
                String selections = compiledSQL.trim().substring(7); // Skip "SELECT "
                int fromStart = selections.indexOf("FROM");
                if (fromStart == -1)
                {
                    fromStart = selections.indexOf("from");
                }
                selections = selections.substring(0, fromStart).trim();
                String[] selectedFields = StringUtils.split(selections, ",");

                if (selectedFields == null || selectedFields.length == 0)
                {
                    throw new NucleusUserException(LOCALISER.msg("059003", compiledSQL)); // TODO Check message location
                }
                if (selectedFields.length == 1 && selectedFields[0].trim().equals("*"))
                {
                    // SQL Query using * so just return since all possible fields will be selected
                    return compiledSQL;
                }

                // Generate id column field information for later checking the id is present
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    // Datastore identity - no field to check
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    // Application identity - need to have PK fields selected as minimum
                    String[] idFieldNames = cmd.getPrimaryKeyMemberNames();
                    boolean[] idFieldMissing = null;
                    idFieldMissing = new boolean[idFieldNames.length];
                    for (int i=0;i<idFieldMissing.length;i++)
                    {
                        idFieldMissing[i] = true;
                    }

                    // Go through the selected fields and check the presence of id fields in the SELECT
                    for (int i = 0; i < selectedFields.length; i++)
                    {
                        String fieldName = selectedFields[i].trim();
                        if (fieldName.indexOf(" AS ") > 0)
                        {
                            // Allow for user specification of "XX.YY AS ZZ"
                            fieldName = fieldName.substring(fieldName.indexOf(" AS ")+4).trim();
                        }
                        else if (fieldName.indexOf(" as ") > 0)
                        {
                            // Allow for user specification of "XX.YY as ZZ"
                            fieldName = fieldName.substring(fieldName.indexOf(" as ")+4).trim();
                        }

                        for (int j=0; j<idFieldNames.length; j++)
                        {
                            if (idFieldNames[j].equals(fieldName))
                            {
                                idFieldMissing[i] = false;
                            }
                        }
                    }
                    for (int i = 0; i < idFieldMissing.length; i++)
                    {
                        if (idFieldMissing[i])
                        {
                            throw new NucleusUserException(LOCALISER.msg("059013",  // TODO Check message location
                                compiledSQL, candidateClass.getName(), idFieldNames[i]));
                        }
                    }
                }
            }
        }
        return compiledSQL;
    }
}
