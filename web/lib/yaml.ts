import { stringify } from "yaml";

// `raw` is documented as the raw YAML text as stored, but the web app
// currently submits JSON bodies to /apply (valid YAML, just not formatted as
// such). Re-render it as real YAML for display; if it's not JSON, it's
// presumably already YAML (e.g. applied via CLI), so leave it as-is.
export function toYaml(raw: string): string {
  try {
    return stringify(JSON.parse(raw));
  } catch {
    return raw;
  }
}
