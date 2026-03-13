package datahub.hive.consumer.util;

import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplate;
import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.client.kafka.KafkaEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for MCPEmitterUtil.
 * Tests the emission of Metadata Change Proposals (MCPs) to VDC via Kafka.
 * Uses pure unit testing approach with complete mocking to avoid SSL configuration issues.
 */
@ExtendWith(MockitoExtension.class)
public class MCPEmitterUtilTest {

    @Mock
    private KafkaEmitter mockKafkaEmitter;

    private DataTemplate<DataMap> testAspect;

    @Mock
    private Future<MetadataWriteResponse> mockFuture;

    @Mock
    private MetadataWriteResponse mockSuccessResponse;

    @Mock
    private MetadataWriteResponse mockFailureResponse;

    private static final String ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,testdb.test_table,DEV)";
    private static final String ENTITY_TYPE = "dataset";
    private static final String FAILURE_MESSAGE = "Kafka broker rejected the message";

    @BeforeEach
    void setUp() {
        // Reset all mocks before each test
        reset(mockKafkaEmitter, mockFuture, mockSuccessResponse, mockFailureResponse);
        
        // Create a real DataHub aspect instead of mocking it
        // Using Status aspect as it's simple and commonly used
        testAspect = new Status().setRemoved(false);
    }

    /**
     * Test Case 1: Success Path
     * Tests that when the Future completes successfully and response.isSuccess() returns true,
     * no exception is thrown and the method completes normally.
     */
    @Test
    void emitMCP_ShouldCompleteSuccessfully_WhenEmitIsSuccessful() throws Exception {
        // Arrange
        when(mockKafkaEmitter.emit(any(MetadataChangeProposalWrapper.class), any(Callback.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockSuccessResponse);
        when(mockSuccessResponse.isSuccess()).thenReturn(true);

        // Act & Assert
        assertDoesNotThrow(() -> MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter));

        // Verify interactions
        verify(mockKafkaEmitter, times(1)).emit(any(MetadataChangeProposalWrapper.class), any(Callback.class));
        verify(mockFuture, times(1)).get();
        verify(mockSuccessResponse, times(1)).isSuccess(); // Called once in main method
    }

    /**
     * Test Case 2: Response Failure Path
     * Tests that when Kafka accepts the message but reports failure in the response,
     * an IOException is thrown with the appropriate error message.
     */
    @Test
    void emitMCP_ShouldThrowIOException_WhenResponseIsNotSuccess() throws Exception {
        // Arrange
        when(mockKafkaEmitter.emit(any(MetadataChangeProposalWrapper.class), any(Callback.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockFailureResponse);
        when(mockFailureResponse.isSuccess()).thenReturn(false);
        when(mockFailureResponse.getResponseContent()).thenReturn(FAILURE_MESSAGE);

        // Act & Assert
        IOException exception = assertThrows(IOException.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter);
        });

        // Verify exception message
        assertTrue(exception.getMessage().contains(FAILURE_MESSAGE));
        assertTrue(exception.getMessage().contains("Failed to emit MCP"));

        // Verify interactions
        verify(mockKafkaEmitter, times(1)).emit(any(MetadataChangeProposalWrapper.class), any(Callback.class));
        verify(mockFuture, times(1)).get();
        verify(mockFailureResponse, times(1)).isSuccess();
        verify(mockFailureResponse, times(2)).getResponseContent(); // Called twice: once for logging, once for exception message
    }

    /**
     * Test Case 3: ExecutionException Path
     * Tests that when future.get() throws ExecutionException,
     * it's caught and wrapped in an IOException with the original exception as cause.
     */
    @Test
    void emitMCP_ShouldThrowIOException_WhenFutureGetThrowsExecutionException() throws Exception {
        // Arrange
        ExecutionException executionException = new ExecutionException("Connection timed out", new RuntimeException("Network error"));
        when(mockKafkaEmitter.emit(any(MetadataChangeProposalWrapper.class), any(Callback.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get()).thenThrow(executionException);

        // Act & Assert
        IOException exception = assertThrows(IOException.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter);
        });

        // Verify exception details
        assertEquals("Error emitting MCP", exception.getMessage());
        assertEquals(executionException, exception.getCause());

        // Verify interactions
        verify(mockKafkaEmitter, times(1)).emit(any(MetadataChangeProposalWrapper.class), any(Callback.class));
        verify(mockFuture, times(1)).get();
    }

    /**
     * Test Case 4: InterruptedException Path
     * Tests that when future.get() throws InterruptedException,
     * it's caught, thread is interrupted, and wrapped in an IOException.
     */
    @Test
    void emitMCP_ShouldThrowIOException_WhenFutureGetThrowsInterruptedException() throws Exception {
        // Arrange
        InterruptedException interruptedException = new InterruptedException("Thread was interrupted");
        when(mockKafkaEmitter.emit(any(MetadataChangeProposalWrapper.class), any(Callback.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get()).thenThrow(interruptedException);

        // Act & Assert
        IOException exception = assertThrows(IOException.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter);
        });

        // Verify exception details
        assertEquals("Error emitting MCP", exception.getMessage());
        assertEquals(interruptedException, exception.getCause());

        // Verify thread interruption status
        assertTrue(Thread.currentThread().isInterrupted());

        // Clear the interrupted status for other tests
        Thread.interrupted();

        // Verify interactions
        verify(mockKafkaEmitter, times(1)).emit(any(MetadataChangeProposalWrapper.class), any(Callback.class));
        verify(mockFuture, times(1)).get();
    }

    /**
     * Test Case 5: Callback Success Behavior
     * Tests that the callback's onCompletion method behaves correctly for successful responses.
     * Uses ArgumentCaptor to capture and test the callback logic.
     */
    @Test
    void callback_ShouldLogSuccess_OnSuccessfulCompletion() throws Exception {
        // Arrange
        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        when(mockKafkaEmitter.emit(any(MetadataChangeProposalWrapper.class), any(Callback.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockSuccessResponse);
        when(mockSuccessResponse.isSuccess()).thenReturn(true);

        try (MockedStatic<TimeUtils> mockedTimeUtils = mockStatic(TimeUtils.class)) {
            mockedTimeUtils.when(() -> TimeUtils.calculateDuration(anyLong())).thenReturn(150L);

            // Act
            assertDoesNotThrow(() -> MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter));

            // Capture the callback
            verify(mockKafkaEmitter).emit(any(MetadataChangeProposalWrapper.class), callbackCaptor.capture());
            Callback capturedCallback = callbackCaptor.getValue();

            // Test callback success behavior
            assertDoesNotThrow(() -> capturedCallback.onCompletion(mockSuccessResponse));

            // Verify TimeUtils was called for duration calculation
            mockedTimeUtils.verify(() -> TimeUtils.calculateDuration(anyLong()), times(1));
        }
    }

    /**
     * Test Case 6: Callback Failure Behavior
     * Tests that the callback's onCompletion method behaves correctly for failed responses.
     */
    @Test
    void callback_ShouldLogFailure_OnFailedCompletion() throws Exception {
        // Arrange
        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        when(mockKafkaEmitter.emit(any(MetadataChangeProposalWrapper.class), any(Callback.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockSuccessResponse);
        when(mockSuccessResponse.isSuccess()).thenReturn(true);

        // Act
        assertDoesNotThrow(() -> MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter));

        // Capture the callback
        verify(mockKafkaEmitter).emit(any(MetadataChangeProposalWrapper.class), callbackCaptor.capture());
        Callback capturedCallback = callbackCaptor.getValue();

        // Test callback failure behavior
        when(mockFailureResponse.isSuccess()).thenReturn(false);
        when(mockFailureResponse.getResponseContent()).thenReturn(FAILURE_MESSAGE);

        assertDoesNotThrow(() -> capturedCallback.onCompletion(mockFailureResponse));

        // Verify failure response methods were called
        verify(mockFailureResponse, times(1)).isSuccess();
        verify(mockFailureResponse, times(1)).getResponseContent();
    }

    /**
     * Test Case 7: Callback Exception Behavior
     * Tests that the callback's onFailure method behaves correctly when exceptions occur.
     */
    @Test
    void callback_ShouldLogException_OnFailure() throws Exception {
        // Arrange
        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        when(mockKafkaEmitter.emit(any(MetadataChangeProposalWrapper.class), any(Callback.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockSuccessResponse);
        when(mockSuccessResponse.isSuccess()).thenReturn(true);

        // Act
        assertDoesNotThrow(() -> MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter));

        // Capture the callback
        verify(mockKafkaEmitter).emit(any(MetadataChangeProposalWrapper.class), callbackCaptor.capture());
        Callback capturedCallback = callbackCaptor.getValue();

        // Test callback exception behavior
        Throwable testException = new RuntimeException("Network connection failed");
        assertDoesNotThrow(() -> capturedCallback.onFailure(testException));
    }

    /**
     * Test Case 8: Null Parameter Validation
     * Tests that the method handles null parameters appropriately.
     */
    @Test
    void emitMCP_ShouldThrowException_WhenParametersAreNull() {
        // Test null aspect
        assertThrows(Exception.class, () -> {
            MCPEmitterUtil.emitMCP(null, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter);
        });

        // Test null entityUrn
        assertThrows(Exception.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, null, ENTITY_TYPE, mockKafkaEmitter);
        });

        // Test null entityType
        assertThrows(Exception.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, null, mockKafkaEmitter);
        });

        // Test null kafkaEmitter
        assertThrows(Exception.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, null);
        });
    }

    /**
     * Test Case 9: Invalid URN Format
     * Tests that the method handles invalid URN formats appropriately.
     */
    @Test
    void emitMCP_ShouldThrowException_WhenURNIsInvalid() {
        String invalidUrn = "invalid-urn-format";

        assertThrows(Exception.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, invalidUrn, ENTITY_TYPE, mockKafkaEmitter);
        });
    }

    /**
     * Test Case 10: Empty String Parameters
     * Tests that the method handles empty string parameters appropriately.
     */
    @Test
    void emitMCP_ShouldThrowException_WhenParametersAreEmpty() {
        // Test empty entityUrn
        assertThrows(Exception.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, "", ENTITY_TYPE, mockKafkaEmitter);
        });

        // Test empty entityType
        assertThrows(Exception.class, () -> {
            MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, "", mockKafkaEmitter);
        });
    }

    /**
     * Test Case 11: Utility Class Constructor
     * Tests that the utility class has a private constructor and cannot be instantiated.
     */
    @Test
    void testUtilityClassCannotBeInstantiated() throws Exception {
        var constructor = MCPEmitterUtil.class.getDeclaredConstructor();
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        
        constructor.setAccessible(true);
        assertDoesNotThrow(() -> constructor.newInstance());
    }

    /**
     * Test Case 12: MetadataChangeProposalWrapper Creation
     * Tests that the method properly creates MetadataChangeProposalWrapper with correct parameters.
     */
    @Test
    void emitMCP_ShouldCreateCorrectMCPWrapper() throws Exception {
        // Arrange
        ArgumentCaptor<MetadataChangeProposalWrapper> mcpCaptor = ArgumentCaptor.forClass(MetadataChangeProposalWrapper.class);
        when(mockKafkaEmitter.emit(any(MetadataChangeProposalWrapper.class), any(Callback.class)))
                .thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockSuccessResponse);
        when(mockSuccessResponse.isSuccess()).thenReturn(true);

        // Act
        assertDoesNotThrow(() -> MCPEmitterUtil.emitMCP(testAspect, ENTITY_URN, ENTITY_TYPE, mockKafkaEmitter));

        // Verify MCP wrapper creation
        verify(mockKafkaEmitter).emit(mcpCaptor.capture(), any(Callback.class));
        MetadataChangeProposalWrapper capturedMCP = mcpCaptor.getValue();

        // Verify MCP wrapper properties
        assertNotNull(capturedMCP);
        assertEquals(ENTITY_URN, capturedMCP.getEntityUrn());
        assertEquals(ENTITY_TYPE, capturedMCP.getEntityType());
        assertEquals(testAspect, capturedMCP.getAspect());
    }
}
