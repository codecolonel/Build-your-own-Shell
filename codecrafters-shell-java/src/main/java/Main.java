import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringJoiner;

public class Main {
    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        Path currentDirectory = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        List<BackgroundJob> backgroundJobs = new ArrayList<>();

        while (true) {
            reapExitedJobs(backgroundJobs, System.out);
            System.out.print("$ ");
            String input = sc.nextLine();
            if (input.isBlank()) {
                continue;
            }

            List<String> tokens = parseCommandLine(input);
            if (tokens.isEmpty()) {
                continue;
            }

            List<List<String>> pipelineSegments = splitByPipe(tokens);
            if (pipelineSegments.size() > 1) {
                List<ParsedCommand> parsedSegments = new ArrayList<>();
                for (List<String> segment : pipelineSegments) {
                    if (segment.isEmpty()) {
                        parsedSegments = null;
                        break;
                    }
                    parsedSegments.add(parseRedirection(segment));
                }

                if (parsedSegments == null || parsedSegments.isEmpty()) {
                    continue;
                }

                List<String> lastArgs = parsedSegments.get(parsedSegments.size() - 1).args();
                boolean runInBackground = !lastArgs.isEmpty() && lastArgs.get(lastArgs.size() - 1).equals("&");
                if (runInBackground) {
                    lastArgs.remove(lastArgs.size() - 1);
                }

                try {
                    Path[] currentDirectoryRef = { currentDirectory };
                    executePipeline(parsedSegments, currentDirectoryRef, runInBackground, backgroundJobs, input);
                    currentDirectory = currentDirectoryRef[0];
                } catch (PipelineCommandNotFoundException e) {
                    System.out.println(e.getMessage());
                }
                continue;
            }

            ParsedCommand parsed = parseRedirection(tokens);
            List<String> parts = parsed.args();
            if (parts.isEmpty()) {
                continue;
            }
            boolean runInBackground = parts.get(parts.size() - 1).equals("&");
            if (runInBackground) {
                parts.remove(parts.size() - 1);
                if (parts.isEmpty()) {
                    continue;
                }
            }

            Path redirectStdoutPath = resolveRedirectPath(parsed.redirectStdoutTo(), currentDirectory);

            Path redirectStderrPath = resolveRedirectPath(parsed.redirectStderrTo(), currentDirectory);
            if (redirectStderrPath != null) {
                prepareStderrRedirectFile(redirectStderrPath, parsed.redirectStderrAppend());
            }

            PrintStream output = System.out;
            if (redirectStdoutPath != null) {
                List<StandardOpenOption> stdoutOptions = new ArrayList<>();
                stdoutOptions.add(StandardOpenOption.CREATE);
                stdoutOptions.add(StandardOpenOption.WRITE);
                if (parsed.redirectStdoutAppend()) {
                    stdoutOptions.add(StandardOpenOption.APPEND);
                } else {
                    stdoutOptions.add(StandardOpenOption.TRUNCATE_EXISTING);
                }
                output = new PrintStream(Files.newOutputStream(
                        redirectStdoutPath,
                        stdoutOptions.toArray(new StandardOpenOption[0])
                ));
            }

            String command = parts.get(0);

            try {
                if (isBuiltin(command)) {
                    Path[] currentDirectoryRef = { currentDirectory };
                    if (executeBuiltin(parts, output, currentDirectoryRef, backgroundJobs)) {
                        return;
                    }
                    currentDirectory = currentDirectoryRef[0];
                } else {
                    Path executable = findExecutable(command);
                    if (executable == null) {
                        output.println(input + ": command not found");
                        continue;
                    }

                    List<String> processCommand = new ArrayList<>();
                    processCommand.addAll(parts);

                    ProcessBuilder pb = new ProcessBuilder(processCommand)
                            .directory(currentDirectory.toFile());

                    configureStdoutRedirect(pb, redirectStdoutPath, parsed.redirectStdoutAppend());
                    configureStderrRedirect(pb, redirectStderrPath, parsed.redirectStderrAppend());

                    Process process = pb.start();
                    if (runInBackground) {
                        int jobNumber = nextAvailableJobNumber(backgroundJobs);
                        output.println("[" + jobNumber + "] " + process.pid());
                        backgroundJobs.add(new BackgroundJob(jobNumber, process, input));
                    } else {
                        process.waitFor();
                    }
                }
            } finally {
                if (output != System.out) {
                    output.close();
                }
            }
        }
    }

    private static boolean isBuiltin(String command) {
        return command.equals("exit") ||
                command.equals("cd") ||
                command.equals("pwd") ||
                command.equals("echo") ||
                command.equals("type") ||
                command.equals("jobs");
    }

    private static boolean executeBuiltin(
            List<String> parts,
            PrintStream output,
            Path[] currentDirectoryRef,
            List<BackgroundJob> backgroundJobs
    ) throws Exception {
        String command = parts.get(0);

        if (command.equals("exit")) {
            return true;
        } else if (command.equals("pwd")) {
            output.println(currentDirectoryRef[0]);
        } else if (command.equals("cd")) {
            String target = parts.size() > 1 ? parts.get(1) : "";

            if (target.equals("~")) {
                String home = System.getenv("HOME");
                if (home == null || home.isBlank() || !Files.isDirectory(Path.of(home))) {
                    output.println("cd: " + target + ": No such file or directory");
                    return false;
                }
                currentDirectoryRef[0] = Path.of(home).toAbsolutePath().normalize();
                return false;
            }

            Path targetPath = Path.of(target);
            Path resolvedPath;

            if (targetPath.isAbsolute()) {
                resolvedPath = targetPath.toAbsolutePath().normalize();
            } else {
                resolvedPath = currentDirectoryRef[0].resolve(targetPath).normalize();
            }

            if (Files.isDirectory(resolvedPath)) {
                currentDirectoryRef[0] = resolvedPath.normalize();
            } else {
                output.println("cd: " + target + ": No such file or directory");
            }
        } else if (command.equals("echo")) {
            StringJoiner echoOutput = new StringJoiner(" ");
            for (int i = 1; i < parts.size(); i++) {
                echoOutput.add(parts.get(i));
            }
            output.println(echoOutput);
        } else if (command.equals("type")) {
            String target = parts.size() > 1 ? parts.get(1) : "";
            if (target.equals("exit") ||
                target.equals("cd") ||
                target.equals("pwd") ||
                target.equals("echo") ||
                target.equals("type") ||
                target.equals("jobs")) {

                output.println(target + " is a shell builtin");
            } else {
                Path executable = findExecutable(target);

                if (executable != null) {
                    output.println(target + " is " + executable);
                } else {
                    output.println(target + ": not found");
                }
            }
        } else if (command.equals("jobs")) {
            List<BackgroundJob> toRemove = new ArrayList<>();
            for (int i = 0; i < backgroundJobs.size(); i++) {
                BackgroundJob job = backgroundJobs.get(i);
                String marker = " ";
                if (i == backgroundJobs.size() - 1) {
                    marker = "+";
                } else if (i == backgroundJobs.size() - 2) {
                    marker = "-";
                }

                boolean alive = job.process().isAlive();
                if (alive) {
                    try {
                        if (job.process().waitFor(10, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                            alive = false;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                String statusStr = alive ? "Running" : "Done";
                String displayCommand = job.command().trim();
                if (!alive) {
                    if (displayCommand.endsWith("&")) {
                        displayCommand = displayCommand.substring(0, displayCommand.length() - 1).trim();
                    }
                    toRemove.add(job);
                }

                output.println("[" + job.number() + "]" + marker + "  " + statusStr + "                 " + displayCommand);
            }
            backgroundJobs.removeAll(toRemove);
        }

        return false;
    }

    private static PrintStream createStdoutStream(ParsedCommand parsed, Path currentDirectory) throws Exception {
        Path redirectStdoutPath = resolveRedirectPath(parsed.redirectStdoutTo(), currentDirectory);
        if (redirectStdoutPath == null) {
            return System.out;
        }

        List<StandardOpenOption> stdoutOptions = new ArrayList<>();
        stdoutOptions.add(StandardOpenOption.CREATE);
        stdoutOptions.add(StandardOpenOption.WRITE);
        if (parsed.redirectStdoutAppend()) {
            stdoutOptions.add(StandardOpenOption.APPEND);
        } else {
            stdoutOptions.add(StandardOpenOption.TRUNCATE_EXISTING);
        }
        return new PrintStream(Files.newOutputStream(
                redirectStdoutPath,
                stdoutOptions.toArray(new StandardOpenOption[0])
        ));
    }

    private static void drainInputStream(InputStream input) {
        try {
            input.transferTo(OutputStream.nullOutputStream());
        } catch (Exception ignored) {
        }
    }

    private static Path findExecutable(String name) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null || pathEnv.isEmpty()) {
            return null;
        }

        for (String dir : pathEnv.split(File.pathSeparator)) {
            Path path = Path.of(dir, name);
            if (Files.exists(path) && Files.isExecutable(path)) {
                return path;
            }
        }

        return null;
    }

    private static List<List<String>> splitByPipe(List<String> tokens) {
        List<List<String>> segments = new ArrayList<>();
        List<String> current = new ArrayList<>();

        for (String token : tokens) {
            if (token.equals("|")) {
                segments.add(current);
                current = new ArrayList<>();
            } else {
                current.add(token);
            }
        }
        segments.add(current);
        return segments;
    }

    private static Path resolveRedirectPath(String target, Path currentDirectory) {
        if (target == null) {
            return null;
        }
        Path path = Path.of(target);
        return path.isAbsolute() ? path : currentDirectory.resolve(path).normalize();
    }

    private static void prepareStderrRedirectFile(Path redirectStderrPath, boolean append) throws Exception {
        Files.newOutputStream(
                redirectStderrPath,
                StandardOpenOption.CREATE,
                append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        ).close();
    }

    private static void configureStdoutRedirect(ProcessBuilder pb, Path redirectStdoutPath, boolean append) {
        if (redirectStdoutPath != null) {
            if (append) {
                pb.redirectOutput(ProcessBuilder.Redirect.appendTo(redirectStdoutPath.toFile()));
            } else {
                pb.redirectOutput(redirectStdoutPath.toFile());
            }
        } else {
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        }
    }

    private static void configureStderrRedirect(ProcessBuilder pb, Path redirectStderrPath, boolean append) {
        if (redirectStderrPath != null) {
            if (append) {
                pb.redirectError(ProcessBuilder.Redirect.appendTo(redirectStderrPath.toFile()));
            } else {
                pb.redirectError(redirectStderrPath.toFile());
            }
        } else {
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        }
    }

    private static void executePipeline(
            List<ParsedCommand> segments,
            Path[] currentDirectoryRef,
            boolean runInBackground,
            List<BackgroundJob> backgroundJobs,
            String input
    ) throws Exception {
        for (ParsedCommand parsed : segments) {
            List<String> parts = parsed.args();
            if (parts.isEmpty()) {
                throw new PipelineCommandNotFoundException(input + ": command not found");
            }

            String command = parts.get(0);
            if (!isBuiltin(command) && findExecutable(command) == null) {
                throw new PipelineCommandNotFoundException(input + ": command not found");
            }
        }

        boolean hasBuiltin = false;
        for (ParsedCommand parsed : segments) {
            if (isBuiltin(parsed.args().get(0))) {
                hasBuiltin = true;
                break;
            }
        }

        if (!hasBuiltin) {
            executeExternalPipeline(segments, currentDirectoryRef[0], runInBackground, backgroundJobs, input);
            return;
        }

        executePipelineWithBuiltins(segments, currentDirectoryRef, runInBackground, backgroundJobs, input);
    }

    private static void executeExternalPipeline(
            List<ParsedCommand> segments,
            Path currentDirectory,
            boolean runInBackground,
            List<BackgroundJob> backgroundJobs,
            String input
    ) throws Exception {
        List<ProcessBuilder> builders = new ArrayList<>();

        for (int i = 0; i < segments.size(); i++) {
            ParsedCommand parsed = segments.get(i);
            List<String> parts = parsed.args();

            ProcessBuilder pb = new ProcessBuilder(parts).directory(currentDirectory.toFile());

            Path redirectStderrPath = resolveRedirectPath(parsed.redirectStderrTo(), currentDirectory);
            if (redirectStderrPath != null) {
                prepareStderrRedirectFile(redirectStderrPath, parsed.redirectStderrAppend());
            }
            configureStderrRedirect(pb, redirectStderrPath, parsed.redirectStderrAppend());

            if (i == segments.size() - 1) {
                Path redirectStdoutPath = resolveRedirectPath(parsed.redirectStdoutTo(), currentDirectory);
                configureStdoutRedirect(pb, redirectStdoutPath, parsed.redirectStdoutAppend());
            }

            if (i == 0) {
                pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
            }

            builders.add(pb);
        }

        List<Process> processes = ProcessBuilder.startPipeline(builders);
        Process lastProcess = processes.get(processes.size() - 1);

        if (runInBackground) {
            int jobNumber = nextAvailableJobNumber(backgroundJobs);
            System.out.println("[" + jobNumber + "] " + lastProcess.pid());
            backgroundJobs.add(new BackgroundJob(jobNumber, lastProcess, input));
        } else {
            for (Process process : processes) {
                process.waitFor();
            }
        }
    }

    private static void executePipelineWithBuiltins(
            List<ParsedCommand> segments,
            Path[] currentDirectoryRef,
            boolean runInBackground,
            List<BackgroundJob> backgroundJobs,
            String input
    ) throws Exception {
        InputStream previousOutput = null;
        List<Process> processes = new ArrayList<>();
        PrintStream finalStdout = null;

        for (int i = 0; i < segments.size(); i++) {
            ParsedCommand parsed = segments.get(i);
            List<String> parts = parsed.args();
            String command = parts.get(0);
            boolean isFirst = i == 0;
            boolean isLast = i == segments.size() - 1;

            if (isBuiltin(command)) {
                PrintStream stageOutput;
                PipedInputStream pipeReader = null;

                if (!isLast) {
                    pipeReader = new PipedInputStream();
                    PipedOutputStream pipeWriter = new PipedOutputStream(pipeReader);
                    stageOutput = new PrintStream(pipeWriter, true);
                } else {
                    finalStdout = createStdoutStream(parsed, currentDirectoryRef[0]);
                    stageOutput = finalStdout;
                }

                if (!isFirst) {
                    InputStream stdin = previousOutput;
                    Thread drainThread = new Thread(() -> drainInputStream(stdin));
                    drainThread.start();
                    if (executeBuiltin(parts, stageOutput, currentDirectoryRef, backgroundJobs)) {
                        return;
                    }
                    if (!isLast) {
                        stageOutput.close();
                    }
                    drainThread.join();
                } else {
                    if (executeBuiltin(parts, stageOutput, currentDirectoryRef, backgroundJobs)) {
                        return;
                    }
                    if (!isLast) {
                        stageOutput.close();
                    }
                }

                previousOutput = pipeReader;
            } else {
                ProcessBuilder pb = new ProcessBuilder(parts).directory(currentDirectoryRef[0].toFile());

                Path redirectStderrPath = resolveRedirectPath(parsed.redirectStderrTo(), currentDirectoryRef[0]);
                if (redirectStderrPath != null) {
                    prepareStderrRedirectFile(redirectStderrPath, parsed.redirectStderrAppend());
                }
                configureStderrRedirect(pb, redirectStderrPath, parsed.redirectStderrAppend());

                if (isFirst) {
                    pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
                } else {
                    pb.redirectInput(ProcessBuilder.Redirect.PIPE);
                }

                if (isLast) {
                    Path redirectStdoutPath = resolveRedirectPath(parsed.redirectStdoutTo(), currentDirectoryRef[0]);
                    configureStdoutRedirect(pb, redirectStdoutPath, parsed.redirectStdoutAppend());
                } else {
                    pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
                }

                Process process = pb.start();
                processes.add(process);

                if (!isFirst) {
                    InputStream stdin = previousOutput;
                    Thread feederThread = new Thread(() -> {
                        try (OutputStream processStdin = process.getOutputStream()) {
                            stdin.transferTo(processStdin);
                        } catch (Exception ignored) {
                        }
                    });
                    feederThread.start();
                    feederThread.join();
                }

                previousOutput = isLast ? null : process.getInputStream();
            }
        }

        if (runInBackground) {
            Process lastProcess = processes.get(processes.size() - 1);
            int jobNumber = nextAvailableJobNumber(backgroundJobs);
            System.out.println("[" + jobNumber + "] " + lastProcess.pid());
            backgroundJobs.add(new BackgroundJob(jobNumber, lastProcess, input));
        } else {
            for (Process process : processes) {
                process.waitFor();
            }
        }

        if (finalStdout != null && finalStdout != System.out) {
            finalStdout.close();
        }
    }

    private static class PipelineCommandNotFoundException extends Exception {
        PipelineCommandNotFoundException(String message) {
            super(message);
        }
    }

    private static ParsedCommand parseRedirection(List<String> tokens) {
        List<String> args = new ArrayList<>();
        String redirectStdoutTo = null;
        boolean redirectStdoutAppend = false;
        String redirectStderrTo = null;
        boolean redirectStderrAppend = false;

        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);

            if ((token.equals(">>") || token.equals("1>>")) && i + 1 < tokens.size()) {
                redirectStdoutTo = tokens.get(i + 1);
                redirectStdoutAppend = true;
                i++;
                continue;
            }

            if ((token.equals(">") || token.equals("1>")) && i + 1 < tokens.size()) {
                redirectStdoutTo = tokens.get(i + 1);
                redirectStdoutAppend = false;
                i++;
                continue;
            }

            if (token.equals("2>>") && i + 1 < tokens.size()) {
                redirectStderrTo = tokens.get(i + 1);
                redirectStderrAppend = true;
                i++;
                continue;
            }

            if (token.equals("2>") && i + 1 < tokens.size()) {
                redirectStderrTo = tokens.get(i + 1);
                redirectStderrAppend = false;
                i++;
                continue;
            }

            args.add(token);
        }

        return new ParsedCommand(args, redirectStdoutTo, redirectStdoutAppend, redirectStderrTo, redirectStderrAppend);
    }

    private static List<String> parseCommandLine(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean tokenStarted = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (c == '\\' && inDoubleQuote) {
                if (i + 1 < input.length()) {
                    char next = input.charAt(i + 1);
                    if (next == '"' || next == '\\') {
                        current.append(next);
                        tokenStarted = true;
                        i++;
                        continue;
                    }
                    // In double quotes, backslash only escapes specific chars for this stage.
                    current.append('\\');
                    current.append(next);
                    tokenStarted = true;
                    i++;
                    continue;
                }
                current.append('\\');
                tokenStarted = true;
                continue;
            }

            if (c == '\\' && !inSingleQuote && !inDoubleQuote) {
                if (i + 1 < input.length()) {
                    current.append(input.charAt(i + 1));
                    tokenStarted = true;
                    i++;
                } else {
                    current.append('\\');
                    tokenStarted = true;
                }
                continue;
            }

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                tokenStarted = true;
                continue;
            }

            if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                tokenStarted = true;
                continue;
            }

            if (c == '|' && !inSingleQuote && !inDoubleQuote) {
                if (tokenStarted) {
                    tokens.add(current.toString());
                    current.setLength(0);
                    tokenStarted = false;
                }
                tokens.add("|");
                continue;
            }

            if (Character.isWhitespace(c) && !inSingleQuote && !inDoubleQuote) {
                if (tokenStarted) {
                    tokens.add(current.toString());
                    current.setLength(0);
                    tokenStarted = false;
                }
                continue;
            }

            current.append(c);
            tokenStarted = true;
        }

        if (tokenStarted) {
            tokens.add(current.toString());
        }

        return tokens;
    }

    private record ParsedCommand(
            List<String> args,
            String redirectStdoutTo,
            boolean redirectStdoutAppend,
            String redirectStderrTo,
            boolean redirectStderrAppend
    ) {}

    private static int nextAvailableJobNumber(List<BackgroundJob> backgroundJobs) {
        boolean[] used = new boolean[backgroundJobs.size() + 2];
        for (BackgroundJob job : backgroundJobs) {
            int number = job.number();
            if (number >= 0 && number < used.length) {
                used[number] = true;
            }
        }
        for (int i = 1; i < used.length; i++) {
            if (!used[i]) {
                return i;
            }
        }
        return used.length;
    }

    private static void reapExitedJobs(List<BackgroundJob> backgroundJobs, PrintStream output) {
        List<BackgroundJob> toRemove = new ArrayList<>();
        for (int i = 0; i < backgroundJobs.size(); i++) {
            BackgroundJob job = backgroundJobs.get(i);
            boolean alive = job.process().isAlive();
            if (alive) {
                try {
                    if (job.process().waitFor(10, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                        alive = false;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (!alive) {
                String marker = " ";
                if (i == backgroundJobs.size() - 1) {
                    marker = "+";
                } else if (i == backgroundJobs.size() - 2) {
                    marker = "-";
                }

                String displayCommand = job.command().trim();
                if (displayCommand.endsWith("&")) {
                    displayCommand = displayCommand.substring(0, displayCommand.length() - 1).trim();
                }
                toRemove.add(job);
                output.println("[" + job.number() + "]" + marker + "  Done                 " + displayCommand);
            }
        }
        backgroundJobs.removeAll(toRemove);
    }

    private record BackgroundJob(
            int number,
            Process process,
            String command
    ) {}
}