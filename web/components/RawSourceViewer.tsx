import { toYaml } from "@/lib/yaml";
import RawSourceViewerClient from "@/components/RawSourceViewerClient";

export default function RawSourceViewer({ raw }: { raw: string }) {
  const formatted = toYaml(raw);

  return <RawSourceViewerClient formatted={formatted} />;
}
