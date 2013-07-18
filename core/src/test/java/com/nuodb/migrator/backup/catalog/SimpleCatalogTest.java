package com.nuodb.migrator.backup.catalog;

import com.nuodb.migrator.backup.format.csv.CsvAttributes;
import com.nuodb.migrator.spec.ResourceSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.commons.io.FileUtils.getTempDirectoryPath;
import static org.junit.Assert.fail;

public class SimpleCatalogTest {

//    private SimpleCatalog simpleCatalog;
//    private CatalogWriter writer;

    @Before
    public void setUp() throws Exception {
        String fileCatalogPath = getTempDirectoryPath();
        ResourceSpec outputSpec = new ResourceSpec();
        outputSpec.setPath(fileCatalogPath);
        outputSpec.setType(CsvAttributes.FORMAT);

//        simpleCatalog = new SimpleCatalog(outputSpec.getPath());
//        assertEquals(simpleCatalog.getPath(), fileCatalogPath);
    }

    @Test
    public void testOpen() throws Exception {
        try {
//            writer = simpleCatalog.getCatalogWriter();
        } catch (CatalogException exception) {
            fail(exception.getMessage());
        }
    }

    @After
    public void tearDown() throws Exception {
//        if (writer != null) {
//            writer.close();
//        }
//        File file = simpleCatalog.getCatalogDir();
//        if (file != null && file.exists()) {
//            forceDelete(file);
//        }
    }
}
