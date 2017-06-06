package com.inspiring.pugtsdb.creation.pug;

import com.inspiring.pugtsdb.PugTSDB;
import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import cucumber.api.java.After;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class PugCreationSteps {

    private String actualStorage;
    private PugTSDB actualPug;
    private Exception actualException;

    @After
    public void cleanup() {
        if (actualPug != null) {
            actualPug.close();
        }

        if (actualStorage != null) {
            Path storagePath = Paths.get(actualStorage);
            Path storageParent = storagePath.getParent();

            if (storageParent != null) {
                File storageDir = storageParent.toFile();
                String filename = storagePath.getFileName().toString();
                File[] files = storageDir.listFiles((dir, name) -> name.matches(filename + "\\..*"));

                for (File file : files) {
                    file.delete();
                }
            }
        }
    }

    @When("^create a Pug instance with storage path \"([^\"]*)\" user \"([^\"]*)\" and pass \"([^\"]*)\"$")
    public void createAPugInstanceWithStoragePathUserAndPass(String storage, String user, String pass) throws Throwable {
        try {
            actualPug = new PugTSDB(actualStorage = storage, user, pass);
        } catch (Exception e) {
            actualException = e;
        }
    }

    @When("^create a Pug instance with storage path null user \"([^\"]*)\" and pass \"([^\"]*)\"$")
    public void createAPugInstanceWithStoragePathNullUserAndPass(String user, String pass) throws Throwable {
        try {
            actualPug = new PugTSDB(null, user, pass);
        } catch (Exception e) {
            actualException = e;
        }
    }

    @When("^create a Pug instance with storage path \"([^\"]*)\" user null and pass \"([^\"]*)\"$")
    public void createAPugInstanceWithStoragePathUserNullAndPass(String storage, String pass) throws Throwable {
        try {
            actualPug = new PugTSDB(actualStorage = storage, null, pass);
        } catch (Exception e) {
            actualException = e;
        }
    }

    @When("^create a Pug instance with storage path \"([^\"]*)\" user \"([^\"]*)\" and pass null$")
    public void createAPugInstanceWithStoragePathUserAndPassNull(String storage, String user) throws Throwable {
        try {
            actualPug = new PugTSDB(actualStorage = storage, user, null);
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
