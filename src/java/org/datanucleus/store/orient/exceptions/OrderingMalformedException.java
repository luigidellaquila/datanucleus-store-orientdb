package org.datanucleus.store.orient.exceptions;

import org.datanucleus.exceptions.NucleusException;

public class OrderingMalformedException extends NucleusException
{

    /** Constructor for Exception when ordering is wrong specified */
    public OrderingMalformedException(String keyword)
    {
        super("Ordering has to be specified with " + "asc, desc, ascending or " + "descending but was specified with: " + keyword);
    }
}
