// Hard floor: the CLI (and its daemons) assumes Node >= 22. Imported FIRST
// from the entrypoint so this runs before any other module body — an old
// runtime gets a readable message instead of a deep stack trace from some
// API that doesn't exist yet.
const major = Number(process.versions.node.split('.')[0]);
if (!Number.isFinite(major) || major < 22) {
  console.error(
    `colab-cli requires Node.js >= 22 (running on ${process.version}).\n` +
      `Please upgrade: https://nodejs.org/  (or \`nvm install 22\`)`,
  );
  process.exit(1);
}
