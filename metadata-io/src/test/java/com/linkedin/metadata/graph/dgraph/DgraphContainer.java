package com.linkedin.metadata.graph.dgraph;

import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.NonNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.stream.Collectors.toSet;

public class DgraphContainer extends GenericContainer<DgraphContainer> {

    /**
     * The image defaults to the official Dgraph image: <a href="https://hub.docker.com/_/dgraph/dgraph">Dgraph</a>.
     */
    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("dgraph/dgraph");

    private static final int HTTP_PORT = 8080;

    private static final int GRPC_PORT = 9080;

    private boolean started = false;

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        started = true;
    }

    @Override
    protected void containerIsStopped(InspectContainerResponse containerInfo) {
        super.containerIsStopped(containerInfo);
        started = false;
    }

    private final Map<String, String> zeroArguments = new HashMap<>();

    private final Map<String, String> alphaArguments = new HashMap<>();

    /**
     * Creates a DgraphContainer using a specific docker image. Connect the container
     * to another DgraphContainer to form a cluster via `peerAlias`.
     *
     * @param dockerImageName The docker image to use.
     */
    public DgraphContainer(@NonNull final DockerImageName dockerImageName) {
        super(dockerImageName);

        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        WaitStrategy waitForLeader = new LogMessageWaitStrategy()
            .withRegEx(".* Got Zero leader: .*\n");
        WaitStrategy waitForCluster = new LogMessageWaitStrategy()
            .withRegEx(".* Server is ready\n");
        WaitStrategy waitForHttp = new HttpWaitStrategy()
            .forPort(HTTP_PORT)
            .forStatusCodeMatching(response -> response == HTTP_OK);

        this.waitStrategy = new WaitAllStrategy()
            .withStrategy(waitForLeader)
            .withStrategy(waitForCluster)
            .withStrategy(waitForHttp)
            .withStartupTimeout(Duration.ofMinutes(1));

        if (dockerImageName.getVersionPart().compareTo("v21.03.0") < 0) {
            withAlphaArgument("whitelist", "0.0.0.0/0");
        } else {
            withAlphaArgumentValues("security", "whitelist=0.0.0.0/0");
        }

        addExposedPorts(HTTP_PORT, GRPC_PORT);
    }

    /**
     * Adds an argument to the zero command.
     *
     * @param argument name of the argument
     * @param value value, null if argument is a flag
     * @return this
     */
    public DgraphContainer withZeroArgument(@NonNull String argument, String value) {
        addArgument(zeroArguments, argument, value);
        return this;
    }

    /**
     * Adds a value to an argument list to the zero command.
     *
     * Some arguments of the zero command form a list of values, e.g. `audit` or `raft`.
     * These values are separated by a ";". Setting multiple values for those arguments should
     * be done via this method.
     *
     * @param argument name of the argument
     * @param values values to add to the argument
     * @return this
     */
    public DgraphContainer withZeroArgumentValues(@NonNull String argument, @NonNull String... values) {
        addArgumentValues(zeroArguments, argument, values);
        return this;
    }

    /**
     * Adds an argument to the alpha command.
     *
     * @param argument name of the argument
     * @param value value, null if argument is a flag
     * @return this
     */
    public DgraphContainer withAlphaArgument(@NonNull String argument, String value) {
        addArgument(alphaArguments, argument, value);
        return this;
    }

    /**
     * Adds a value to an argument list to the alpha command.
     *
     * Some arguments of the alpha command form a list of values, e.g. `audit` or `raft`.
     * These values are separated by a ";". Setting multiple values for those arguments should
     * be done via this method.
     *
     * @param argument name of the argument
     * @param values values to add to the argument
     * @return this
     */
    public DgraphContainer withAlphaArgumentValues(@NonNull String argument, @NonNull String... values) {
        addArgumentValues(alphaArguments, argument, values);
        return this;
    }

    private void addArgument(Map<String, String> arguments, @NonNull String argument, String value) {
        if (started) {
            throw new IllegalStateException("The container started already, cannot amend command arguments");
        }

        arguments.put(argument, value);
    }

    private void addArgumentValues(Map<String, String> arguments, @NonNull String argument, @NonNull String... values) {
        if (started) {
            throw new IllegalStateException("The container started already, cannot amend command arguments");
        }

        StringJoiner joiner = new StringJoiner("; ");
        Arrays.stream(values).forEach(joiner::add);
        String value = joiner.toString();

        if (arguments.containsKey(argument)) {
            arguments.put(argument, arguments.get(argument) + "; " + value);
        } else {
            arguments.put(argument, value);
        }
    }

    /**
     * Provides the command used to start the zero process. Command line arguments can be added
     * by calling `withZeroArgument` and `withZeroArgumentValues` before calling this method.
     * @return command string
     */
    public @NonNull String getZeroCommand() {
        return getCommand("dgraph zero", zeroArguments);
    }

    /**
     * Provides the command used to start the alpha process. Command line arguments can be added
     * by calling `withAlphaArgument` and `withAlphaArgumentValues` before calling this method.
     * @return command string
     */
    public @NonNull String getAlphaCommand() {
        return getCommand("dgraph alpha", alphaArguments);
    }

    private @NonNull String getCommand(@NonNull String command, @NonNull Map<String, String> arguments) {
        StringJoiner joiner = new StringJoiner(" --");

        arguments.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(argument -> {
                if (argument.getValue() == null) {
                    return argument.getKey();
                } else {
                    return argument.getKey() + " \"" + argument.getValue() + "\"";
                }
            }).forEach(joiner::add);

        if (joiner.length() == 0) {
            return command;
        } else {
            return command + " --" + joiner;
        }
    }

    @Override
    public void start() {
        String zeroCommand = this.getZeroCommand();
        String alhpaCommand = this.getAlphaCommand();
        this.setCommand("/bin/bash", "-c", zeroCommand + " & " + alhpaCommand);
        super.start();
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Stream.of(getHttpPort(), getGrpcPort())
            .map(this::getMappedPort)
            .collect(toSet());
    }

    @Override
    protected void configure() { }

    public int getHttpPort() {
        return getMappedPort(HTTP_PORT);
    }

    public int getGrpcPort() {
        return getMappedPort(GRPC_PORT);
    }

    public String getHttpUrl() {
        return String.format("http://%s:%d", getHost(), getHttpPort());
    }

    public String getGrpcUrl() {
        return String.format("%s:%d", getHost(), getGrpcPort());
    }

}
