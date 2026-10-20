<script>
	import '@evidence-dev/tailwind/fonts.css';
	import '../app.css';
	import { EvidenceDefaultLayout } from '@evidence-dev/core-components';
	import { page } from '$app/stores';

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

	$: benchmarkStorage =
		/^\/benchmarks\/(duckdb|ducklake)(?:\/|$)/.exec($page.route.id ?? '')?.[1];
</script>

<div
	class:benchmark-duckdb-active={benchmarkStorage === 'duckdb'}
	class:benchmark-ducklake-active={benchmarkStorage === 'ducklake'}
>
	<EvidenceDefaultLayout data={layoutData}>
		<slot slot="content" />
	</EvidenceDefaultLayout>
</div>

<style>
	:global(.benchmark-duckdb-active aside a[href$='/benchmarks/duckdb']),
	:global(.benchmark-ducklake-active aside a[href$='/benchmarks/ducklake']) {
		color: var(--color-primary);
	}
</style>
