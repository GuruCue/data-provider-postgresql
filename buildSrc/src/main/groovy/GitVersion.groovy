/*
 * This file is part of Guru Cue Search & Recommendation Engine.
 * Copyright (C) 2017 Guru Cue Ltd.
 *
 * Guru Cue Search & Recommendation Engine is free software: you can
 * redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * Guru Cue Search & Recommendation Engine is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Guru Cue Search & Recommendation Engine. If not, see
 * <http://www.gnu.org/licenses/>.
 */

class GitVersion {
    String branch
    List<Integer> subversions

    GitVersion(String branch, String version) {
        this.branch = branch
        subversions = new ArrayList<Integer>()
        String[] versionComponents = version.split('\\.')
        String v = versionComponents[0]
        int pos = v.lastIndexOf('_')
        int j = v.lastIndexOf('-')
        if (j > pos) pos = j
        subversions.add(Integer.valueOf(pos < 0 ? v : v.substring(pos+1), 10))
        for (int i = 1; i < versionComponents.length; i++) {
            subversions.add(Integer.valueOf(versionComponents[i], 10))
        }
    }

    GitVersion(String branch, List<Integer> subversions) {
        this.branch = branch
        this.subversions = subversions
    }

    boolean biggerThan(GitVersion v) {
        if (!branch.equals(v.branch)) {
            return branch.compareTo(v.branch) > 0
        }
        final int n = v.subversions.size() > subversions.size() ? subversions.size() : v.subversions.size()
        for (int i = 0; i < n; i++) {
            if (subversions[i] > v.subversions[i]) return true;
        }
        return subversions.size() > v.subversions.size()
    }

    GitVersion incMinor() {
        List<Integer> subversions = new ArrayList<Integer>(this.subversions)
        Integer n = subversions.size() - 1
        if (n >= 0) {
            subversions[n] = subversions[n] + 1
        }
        else {
            subversions.add(1)
        }
        return new GitVersion(branch, subversions)
    }

    String toString() {
        final StringBuilder sb = new StringBuilder()
        if ((branch != null) && (branch.length() > 0)) sb.append(branch)
        final Iterator<Integer> vit = subversions.iterator()
        if (vit.hasNext()) {
            sb.append('-').append(vit.next())
            while (vit.hasNext()) {
                sb.append('.').append(vit.next())
            }
        }
        return sb.toString()
    }

    static GitVersion getVersion() {
        def proc = "git branch".execute()
        StringBuilder sbIn = new StringBuilder()
        StringBuilder sbErr = new StringBuilder()
        proc.consumeProcessOutput(sbIn, sbErr)
        proc.waitFor()
        String err = sbErr.toString().trim()
        if (err.length() > 0) throw new RuntimeException("Git failed: ${err}")
        int pos = sbIn.lastIndexOf('*');
        String branch = ''
        if (pos >= 0) {
            int npos = sbIn.indexOf('\n', pos)
            branch = sbIn.substring(pos+1, npos).trim()
            if ('master'.equals(branch)) branch = ''
        }

        proc = "git tag -l rls_${branch}*".execute()
        sbIn = new StringBuilder()
        sbErr = new StringBuilder()
        proc.consumeProcessOutput(sbIn, sbErr)
        proc.waitFor()
        err = sbErr.toString().trim()
        if ((err.length() > 0) && !'fatal: No names found, cannot describe anything.'.equals(err)) throw new RuntimeException("Git failed: ${err}")
        Thread.sleep(1000L)
        String[] lines = sbIn.toString().split('\r?\n')
        GitVersion v = null;
        if ((lines.length > 1) || ((lines.length == 1) && (lines[0] != ''))) {
            int i = 0;
            while (i < lines.length) {
                try {
                    v = new GitVersion(branch, lines[i++])
                    break;
                }
                catch (Exception e) {
                    println("Ignoring version \"${lines[i-1]}\" due to an exception: " + e.toString())
                }
            }
            while (i < lines.length) {
                try {
                    GitVersion nv = new GitVersion(branch, lines[i++])
                    if (nv.biggerThan(v)) v = nv;
                }
                catch (Exception e) {
                    println("Ignoring version \"${lines[i-1]}\" due to an exception: " + e.toString())
                }
            }
        }
        return v == null ? new GitVersion(branch, [1, 0, 0]) : v
    }
}
