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
package org.datanucleus.store.orient.valuegenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.datanucleus.store.orient.OrientStoreManager;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerationException;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Value generator for Orient that provides increment/sequence like generation. Utilises the persistence of objects of
 * NucleusSequence. Each objects stores the sequence class/field name and the current value of that sequence.
 */
public class OrientIncrementGenerator extends AbstractDatastoreGenerator
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_ORIENT = Localiser.getInstance("org.datanucleus.store.orient.Localisation",
        OrientStoreManager.class.getClassLoader());

    /** Name of the sequence that we are storing values under (name of the class/field). */
    private final String sequenceName;

    private ODatabaseObjectTx container = null;

    /**
     * Constructor.
     * @param name Symbolic name for this generator
     * @param props Properties defining the behaviour of this generator
     */
    public OrientIncrementGenerator(String name, Properties props)
    {
        super(name, props);
        allocationSize = 5;

        if (properties.getProperty("sequence-name") != null)
        {
            // Specified sequence-name so use that
            sequenceName = properties.getProperty("sequence-name");
        }
        else if (properties.getProperty("field-name") != null)
        {
            // Use field name as the sequence name so we have one sequence per field on the class
            sequenceName = properties.getProperty("field-name");
        }
        else
        {
            // Use actual class name as the sequence name so we have one sequence per class
            sequenceName = properties.getProperty("class-name");
        }
    }

    /**
     * Get a new block with the specified number of values.
     * @param number The number of additional values required
     * @return the block
     */
    protected ValueGenerationBlock obtainGenerationBlock(int number)
    {
        ValueGenerationBlock block = null;

        // Try getting the block
        try
        {
            container = (ODatabaseObjectTx) connectionProvider.retrieveConnection().getConnection();

            try
            {
                if (number < 0)
                {
                    block = reserveBlock();
                }
                else
                {
                    block = reserveBlock(number);
                }
            }
            catch (ValueGenerationException poidex)
            {
                NucleusLogger.VALUEGENERATION.info(LOCALISER.msg("040003", poidex.getMessage()));
                throw poidex;
            }
            catch (RuntimeException ex)
            {
                // exceptions cached by the poid should be enclosed in PoidException
                // when the exceptions are not catched exception by poid, we give a new try
                // in creating the repository
                NucleusLogger.VALUEGENERATION.info(LOCALISER.msg("040003", ex.getMessage()));
                throw ex;
            }
        }
        finally
        {
            if (container != null)
            {
                connectionProvider.releaseConnection();
                container = null;
            }
        }

        if (block != null)
        {
            return block;
        }
        return super.obtainGenerationBlock(number);
    }

    /**
     * Method to reserve a block of "size" identities.
     * @param size Block size
     * @return The reserved block
     */
    protected ValueGenerationBlock reserveBlock(long size)
    {
        List ids = new ArrayList();

        // Find the NucleusSequence object in Orient
        NucleusSequence seq = null;
        NucleusSequence baseSeq = new NucleusSequence(sequenceName);
        OrientStoreManager.registerClassInOrient(container, NucleusSequence.class);
        try
        {
            List queryResult = container.query(new OSQLSynchQuery<Object>(
                    "select from NucleusSequence where entityName = '" + sequenceName+"'"));
            if (queryResult != null && queryResult.size() == 1)
            {
                seq = (NucleusSequence) queryResult.get(0);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        Long nextVal = null;
        if (seq == null)
        {
            seq = baseSeq;
            nextVal = Long.valueOf(1);
            seq.setCurrentValue(1);
        }
        else
        {
            nextVal = Long.valueOf(seq.getCurrentValue());
        }
        seq.incrementCurrentValue(allocationSize);
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE.debug(LOCALISER_ORIENT.msg("Orient.ValueGenerator.UpdatingSequence", sequenceName,
                "" + seq.getCurrentValue()));
        }

        for (int i = 0; i < size; i++)
        {
            ids.add(nextVal);
            nextVal = Long.valueOf(nextVal.longValue() + 1);
        }

        container.save(seq);

        return new ValueGenerationBlock(ids);
    }
}
