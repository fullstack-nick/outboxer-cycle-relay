import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Local-only release guard for the public repository. */
public final class PublicationCheck {

    private static final Path ROOT = Path.of("").toAbsolutePath().normalize();
    // Construct the denylist without embedding denied names in the public tree.
    private static final String FORBIDDEN_EXPRESSION = String.join(
            "|",
            "an" + "gel",
            "en" + "gel",
            "en" + "gel" + "global",
            "kar" + "riere\\.at",
            "pla" + "nt-linz",
            "e-" + "connect",
            "pro" + "cess[ -]?ob" + "server");
    private static final Pattern FORBIDDEN = Pattern.compile("(?i)(" + FORBIDDEN_EXPRESSION + ")");
    private static final Pattern PRIVATE_KEY = Pattern.compile("-----BEGIN [A-Z ]*PRIVATE KEY-----");
    private static final Pattern ACCESS_TOKEN = Pattern.compile(
            "(?i)(gh[pousr]_[A-Za-z0-9_]{20,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{20,})");
    private static final Pattern MARKDOWN_LINK = Pattern.compile("\\[[^]\\r\\n]+]\\(([^)\\r\\n]+)\\)");
    private static final Set<String> SENSITIVE_SUFFIXES = Set.of(
            ".pem", ".key", ".p12", ".pfx", ".jks", ".keystore", ".env", ".db", ".db-wal", ".db-shm");

    private static final List<String> REQUIRED_PATHS = List.of(
            "README.md",
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
            "SECURITY.md",
            "docs/architecture.md",
            "docs/reliability-model.md",
            "docs/testing.md",
            "docs/threat-model.md",
            "docs/troubleshooting.md",
            "docs/mock-pull-request.md",
            "docs/adr/0001-disk-backed-edge-outbox.md",
            "docs/adr/0002-at-least-once-and-central-deduplication.md",
            "docs/adr/0003-per-machine-ordering.md",
            "docs/adr/0004-backward-compatible-schema-evolution.md",
            "docs/adr/0005-application-receipts-over-mqtt.md",
            "docs/adr/0006-ordered-kafka-listener-boundary.md",
            "docs/evidence/v1-outage.md",
            "docs/evidence/load.md",
            "docs/evidence/crash-windows.md",
            "docs/evidence/v2-compose-smoke.md",
            "docs/evidence/kind-smoke.md",
            "infra/k8s/base/kustomization.yaml",
            "scripts/verify.ps1",
            "scripts/verify.sh",
            "settings-gradle.lockfile",
            "central-cycle-service/gradle.lockfile",
            "contracts/gradle.lockfile",
            "edge-relay/gradle.lockfile",
            "machine-simulator/gradle.lockfile");

    private PublicationCheck() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 0) {
            throw new IllegalArgumentException("PublicationCheck accepts no arguments");
        }
        require(ROOT.resolve(".git").toFile().exists(), "run from the repository root");

        List<String> tracked = commandLines("git", "ls-files");
        require(!tracked.isEmpty(), "Git has tracked files");
        require(!tracked.contains("INTERNAL_PROJECT_PLAN.md"), "internal planning material is untracked");
        require(tracked.stream().noneMatch(PublicationCheck::isWorkflow), "no CI/CD workflow is tracked");
        require(tracked.stream().noneMatch(PublicationCheck::isSensitivePath), "no sensitive file type is tracked");

        List<String> violations = new ArrayList<>();
        for (String relative : tracked) {
            inspectTrackedFile(relative, violations);
        }
        require(violations.isEmpty(), String.join(System.lineSeparator(), violations));
        pass("tracked content contains no forbidden association or high-confidence secret");

        inspectHistory();
        pass("complete Git history and tag annotations pass the forbidden-term scan");

        for (String required : REQUIRED_PATHS) {
            require(tracked.contains(required), "required public artifact is tracked: " + required);
        }
        String readme = Files.readString(ROOT.resolve("README.md"));
        String notices = Files.readString(ROOT.resolve("THIRD_PARTY_NOTICES.md"));
        String license = Files.readString(ROOT.resolve("LICENSE"));
        require(readme.contains("Independent educational simulation"), "README contains the independence disclaimer");
        require(!readme.contains("Implementation is " + "in progress"), "README has no bootstrap status placeholder");
        require(
                !notices.toLowerCase(Locale.ROOT).contains("will be " + "finalized"),
                "third-party notices are finalized");
        require(license.startsWith("MIT License"), "repository license is MIT");
        require(Files.readString(ROOT.resolve("gradle/wrapper/gradle-wrapper.properties"))
                        .contains("distributionSha256Sum="),
                "Gradle distribution checksum is pinned");
        require(Files.size(ROOT.resolve("gradle/verification-metadata.xml")) > 1_000,
                "dependency verification metadata is populated");
        pass("required portfolio, evidence, license, lock, and verification artifacts are present");

        inspectMarkdownLinks(tracked);
        pass("all local Markdown links resolve");

        ProcessResult whitespace = command("git", "diff", "--check");
        require(whitespace.exitCode == 0, "git diff --check passes: " + whitespace.output);
        String status = String.join("\n", commandLines("git", "status", "--porcelain=v1", "--untracked-files=all"));
        require(status.isBlank(), "working tree is clean before publication: " + status);
        pass("working tree is clean and has no whitespace errors");

        System.out.println("PUBLICATION CHECK PASSED");
    }

    private static void inspectTrackedFile(String relative, List<String> violations) throws IOException {
        Path file = ROOT.resolve(relative).normalize();
        require(file.startsWith(ROOT), "tracked path remains within repository: " + relative);
        byte[] bytes = Files.readAllBytes(file);
        String content = new String(bytes, StandardCharsets.ISO_8859_1);
        if (FORBIDDEN.matcher(content).find()) {
            violations.add("forbidden association in " + relative);
        }
        if (PRIVATE_KEY.matcher(content).find() || ACCESS_TOKEN.matcher(content).find()) {
            violations.add("high-confidence secret in " + relative);
        }
    }

    private static void inspectHistory() throws Exception {
        List<String> revisions = commandLines("git", "rev-list", "--all");
        require(!revisions.isEmpty(), "Git history is non-empty");
        List<String> grepArguments =
                new ArrayList<>(List.of("git", "grep", "-I", "-n", "-i", "-E", FORBIDDEN_EXPRESSION));
        grepArguments.addAll(revisions);
        grepArguments.add("--");
        ProcessResult treeScan = command(grepArguments.toArray(String[]::new));
        require(treeScan.exitCode == 1, "forbidden term found in a Git tree: " + treeScan.output);

        String messages = commandChecked("git", "log", "--all", "--format=%B").output;
        require(!FORBIDDEN.matcher(messages).find(), "commit messages contain no forbidden association");
        String tags = commandChecked("git", "for-each-ref", "--format=%(refname)%00%(contents)", "refs/tags").output;
        require(!FORBIDDEN.matcher(tags).find(), "tag names and annotations contain no forbidden association");
    }

    private static void inspectMarkdownLinks(List<String> tracked) throws IOException {
        List<String> broken = new ArrayList<>();
        for (String relative : tracked) {
            if (!relative.toLowerCase(Locale.ROOT).endsWith(".md")) {
                continue;
            }
            Path markdown = ROOT.resolve(relative);
            Matcher matcher = MARKDOWN_LINK.matcher(Files.readString(markdown));
            while (matcher.find()) {
                String target = matcher.group(1).trim();
                int titleSeparator = target.indexOf(" \"");
                if (titleSeparator > 0) {
                    target = target.substring(0, titleSeparator);
                }
                if (target.startsWith("<") && target.endsWith(">")) {
                    target = target.substring(1, target.length() - 1);
                }
                if (target.startsWith("http://")
                        || target.startsWith("https://")
                        || target.startsWith("mailto:")
                        || target.startsWith("#")) {
                    continue;
                }
                int anchor = target.indexOf('#');
                if (anchor >= 0) {
                    target = target.substring(0, anchor);
                }
                if (target.isBlank()) {
                    continue;
                }
                String decoded = URLDecoder.decode(target, StandardCharsets.UTF_8);
                Path destination = markdown.getParent().resolve(decoded).normalize();
                if (!destination.startsWith(ROOT) || !Files.exists(destination)) {
                    broken.add(relative + " -> " + target);
                }
            }
        }
        require(broken.isEmpty(), "broken local Markdown links: " + String.join(", ", broken));
    }

    private static boolean isWorkflow(String path) {
        String normalized = path.replace('\\', '/').toLowerCase(Locale.ROOT);
        return normalized.startsWith(".github/workflows/");
    }

    private static boolean isSensitivePath(String path) {
        String normalized = path.replace('\\', '/').toLowerCase(Locale.ROOT);
        if (normalized.startsWith(".secrets/") || normalized.contains("/passwords")) {
            return true;
        }
        return SENSITIVE_SUFFIXES.stream().anyMatch(normalized::endsWith);
    }

    private static List<String> commandLines(String... arguments) throws Exception {
        String output = commandChecked(arguments).output;
        return output.isBlank() ? List.of() : Arrays.asList(output.split("\\R"));
    }

    private static ProcessResult commandChecked(String... arguments) throws Exception {
        ProcessResult result = command(arguments);
        if (result.exitCode != 0) {
            throw new IllegalStateException(
                    "Command failed (" + result.exitCode + "): " + String.join(" ", arguments) + "\n" + result.output);
        }
        return result;
    }

    private static ProcessResult command(String... arguments) throws Exception {
        Process process = new ProcessBuilder(arguments)
                .directory(ROOT.toFile())
                .redirectErrorStream(true)
                .start();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        process.getInputStream().transferTo(output);
        int exitCode = process.waitFor();
        return new ProcessResult(exitCode, output.toString(StandardCharsets.UTF_8).trim());
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void pass(String message) {
        System.out.println("PASS: " + message);
    }

    private record ProcessResult(int exitCode, String output) {}
}
