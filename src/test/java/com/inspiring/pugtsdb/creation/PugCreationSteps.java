package com.inspiring.pugtsdb.creation;

import com.inspiring.pugtsdb.PugTSDBOverH2;
import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import cucumber.api.java.After;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class PugCreationSteps {

    private String actualStorage;
    private PugTSDBOverH2 actualPug;
    private Exception actualException;

    @After
    public void cleanup() throws SQLException {
        if (actualPug != null) {
            try (Statement statement = actualPug.getDataSource().getConnection().createStatement()) {
                statement.execute(" DROP ALL OBJECTS DELETE FILES ");
            } finally {
                actualPug.close();
            }
        }
    }

    @When("^create a Pug instance with storage path \"([^\"]*)\" user \"([^\"]*)\" and pass \"([^\"]*)\"$")
    public void createAPugInstanceWithStoragePathUserAndPass(String storage, String user, String pass) throws Throwable {
        try {
            actualPug = new PugTSDBOverH2(actualStorage = storage, user, pass);
        } catch (Exception e) {
            actualException = e;
        }
    }

    @When("^create a Pug instance with storage path null user \"([^\"]*)\" and pass \"([^\"]*)\"$")
    public void createAPugInstanceWithStoragePathNullUserAndPass(String user, String pass) throws Throwable {
        try {
            actualPug = new PugTSDBOverH2(null, user, pass);
        } catch (Exception e) {
            actualException = e;
        }
    }

    @When("^create a Pug instance with storage path \"([^\"]*)\" user null and pass \"([^\"]*)\"$")
    public void createAPugInstanceWithStoragePathUserNullAndPass(String storage, String pass) throws Throwable {
        try {
            actualPug = new PugTSDBOverH2(actualStorage = storage, null, pass);
        } catch (Exception e) {
            actualException = e;
        }
    }

    @When("^create a Pug instance with storage path \"([^\"]*)\" user \"([^\"]*)\" and pass null$")
    public void createAPugInstanceWithStoragePathUserAndPassNull(String storage, String user) throws Throwable {
        try {
            actualPug = new PugTSDBOverH2(actualStorage = storage, user, null);
        } catch (Exception e) {
            actualException = e;
        }
    }

    @Then("^the Pug instance is created successful$")
    public void thePugInstanceIsCreatedSuccessful() throws Throwable {
        assertNull(actualException);
        assertNotNull(actualPug);
    }

    @Then("^an illegal argument exception are thrown$")
    public void anIllegalArgumentExceptionAreThrown() throws Throwable {
        assertNotNull(actualException);
        assertTrue("Exception is not an " + PugIllegalArgumentException.class.getSimpleName() + ": " + actualException.getClass(),
                   actualException instanceof PugIllegalArgumentException);
    }
}
