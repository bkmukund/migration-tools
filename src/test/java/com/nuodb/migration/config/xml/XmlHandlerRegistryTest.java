package com.nuodb.migration.config.xml;


import com.nuodb.migration.config.xml.handler.XmlDumpTaskHandler;
import com.nuodb.migration.config.xml.handler.XmlJdbcConnectionHandler;
import com.nuodb.migration.config.xml.handler.XmlMigrationHandler;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class XmlHandlerRegistryTest {

    private XmlHandlerRegistry xmlHandlerRegistry;

    @Before
    public void setUp() throws Exception {
        xmlHandlerRegistry = new XmlHandlerRegistry();
    }

    @Test
    public void testRegister() throws Exception {
        xmlHandlerRegistry.registerHandler(mock(XmlJdbcConnectionHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlJdbcConnectionHandler.class), 1);
        xmlHandlerRegistry.registerHandler(mock(XmlJdbcConnectionHandler.class), -1);
        xmlHandlerRegistry.registerHandler(mock(XmlDumpTaskHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlMigrationHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlHandlerRegistry.ConverterAdapter.class));
    }

    @Test
    public void testLookupReader() throws Exception {
        XmlReadHandler xmlReadHandler = xmlHandlerRegistry.lookupReader(
                mock(InputNode.class),
                XmlJdbcConnectionHandler.class,
                mock(XmlReadContext.class)
        );
        Assert.assertNull(xmlReadHandler);

        final XmlJdbcConnectionHandler mock = mock(XmlJdbcConnectionHandler.class);

        when(mock.canRead(any(InputNode.class),
                eq(XmlJdbcConnectionHandler.class),
                any(XmlReadContext.class))).thenReturn(true);

        xmlHandlerRegistry.registerHandler(mock);

        xmlReadHandler = xmlHandlerRegistry.lookupReader(
                mock(InputNode.class),
                XmlJdbcConnectionHandler.class,
                mock(XmlReadContext.class)
        );

        Assert.assertNotNull(xmlReadHandler);
        Assert.assertTrue(xmlReadHandler == mock);
        verify(xmlReadHandler).canRead(any(InputNode.class),
                eq(XmlJdbcConnectionHandler.class), any(XmlReadContext.class));
    }


    @Test
    public void testLookupWriter() throws Exception {
        final XmlJdbcConnectionHandler mock = mock(XmlJdbcConnectionHandler.class);

        XmlWriteHandler xmlWriteHandler = xmlHandlerRegistry.lookupWriter(
                mock(Object.class),
                XmlJdbcConnectionHandler.class,
                mock(OutputNode.class),
                mock(XmlWriteContext.class)
        );
        Assert.assertNull(xmlWriteHandler);

        when(mock.canWrite(anyObject(),
                eq(XmlJdbcConnectionHandler.class),
                any(OutputNode.class),
                any(XmlWriteContext.class))).thenReturn(true);
        xmlHandlerRegistry.registerHandler(mock);

        xmlWriteHandler = xmlHandlerRegistry.lookupWriter(
                mock(Object.class),
                XmlJdbcConnectionHandler.class,
                mock(OutputNode.class),
                mock(XmlWriteContext.class)
        );

        Assert.assertNotNull(xmlWriteHandler);
        Assert.assertTrue(xmlWriteHandler == mock);
        verify(xmlWriteHandler).canWrite(anyObject(),
                eq(XmlJdbcConnectionHandler.class),
                any(OutputNode.class), any(XmlWriteContext.class));
    }

    @Test(expected = AssertionError.class)
    public void testRegisterWrongType() throws Exception {
        xmlHandlerRegistry.registerHandler(mock(XmlHandler.class));
    }
}