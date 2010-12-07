package org.datanucleus.store.orient.exceptions;

import org.datanucleus.exceptions.NucleusException;

public class FilterNotParsableException extends NucleusException
{
    /**The filter String which is not parsable*/
    String filter;
    
    
    /**Constructor for the Exception*/
    public FilterNotParsableException(String filter)
    {
        super("Filter malformed: " + filter);
        this.filter = filter;
    }
    
    /**Getter Method for the filter*/
    public String getFilter()
    {
        return filter;
    }
    
}
