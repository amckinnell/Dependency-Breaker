package com.valuablecode;

/**
 * Represents the domain model concept of a network. At each network there may be one or more facilities.
 */
public class Network {

    private String acronym;
	private String id;

    public String getAcronym(){
        return acronym;
    }
    
    public void setAcronym(String acronym){
        this.acronym = acronym;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    
    // -------------------------------------------------------------------------------------------------------
    // Note: most of the Network properties have been removed for the purpose of this exercise.
    // -------------------------------------------------------------------------------------------------------

}
