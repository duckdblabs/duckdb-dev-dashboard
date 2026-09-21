<script>
	import '@evidence-dev/tailwind/fonts.css';
	import '../app.css';
	import { EvidenceDefaultLayout } from '@evidence-dev/core-components';

	export let data;

	function applySidebarTitles(node) {
		if (!node) return node;

		const children = Object.fromEntries(
			Object.entries(node.children ?? {}).map(([key, child]) => [key, applySidebarTitles(child)])
		);
		const sidebarTitle = node.frontMatter?.sidebar_title;

		return {
			...node,
			children,
			frontMatter:
				typeof sidebarTitle === 'string' && sidebarTitle.length > 0
					? { ...node.frontMatter, title: sidebarTitle }
					: node.frontMatter
		};
	}

	$: layoutData = data?.pagesManifest
		? { ...data, pagesManifest: applySidebarTitles(data.pagesManifest) }
		: data;
</script>

<EvidenceDefaultLayout data={layoutData}>
	<slot slot="content" />
</EvidenceDefaultLayout>
