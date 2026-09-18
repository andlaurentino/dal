import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Standalone output keeps the docker image lean: `next build` copies only
  // the files needed to run `node server.js`, no full node_modules layer.
  output: "standalone",
};

export default nextConfig;
